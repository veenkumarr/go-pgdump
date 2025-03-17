package pgdump

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

func chunkSlice(slice []string, chunkSize int) [][]string {
	if chunkSize <= 0 {
		chunkSize = 1
	}
	var chunks [][]string
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

type Dumper struct {
	ConnectionString string
	Parallels        int
	DumpVersion      string
}

func NewDumper(connectionString string, threads int) *Dumper {
	dumpVersion := "0.2.1"
	if threads <= 0 {
		threads = 50
	}
	return &Dumper{ConnectionString: connectionString, Parallels: threads, DumpVersion: dumpVersion}
}

func (d *Dumper) DumpDatabase(outputFile string, opts *TableOptions) error {
	db, err := sql.Open("postgres", d.ConnectionString)
	if err != nil {
		return err
	}
	defer db.Close()

	file, err := os.Create(outputFile)
	if err != nil {
		return err
	}
	defer file.Close()

	info := DumpInfo{
		DumpVersion:   d.DumpVersion,
		ServerVersion: getServerVersion(db),
		CompleteTime:  time.Now().Format("2006-01-02 15:04:05 -0700 MST"),
		ThreadsNumber: d.Parallels,
	}

	if err := writeHeader(file, info); err != nil {
		return err
	}

	// Dump tables
	tables, err := getTables(db, opts)
	if err != nil {
		return err
	}

	var (
		wg sync.WaitGroup
		mx sync.Mutex
	)

	chunks := chunkSlice(tables, d.Parallels)
	for _, chunk := range chunks {
		wg.Add(len(chunk))
		for _, table := range chunk {
			go func(table string) {
				defer wg.Done()
				str, err := scriptTable(db, table)
				if err != nil {
					return
				}
				mx.Lock()
				file.WriteString(str)
				mx.Unlock()
			}(table)
		}
		wg.Wait()
	}

	// Dump views, functions, and procedures (non-table objects)
	views, err := scriptViews(db)
	if err != nil {
		return err
	}
	file.WriteString(views + "\n\n")

	functions, err := scriptFunctionsAndProcedures(db)
	if err != nil {
		return err
	}
	file.WriteString(functions + "\n\n")

	if err := writeFooter(file, info); err != nil {
		return err
	}

	return nil
}

// DumpDBToCSV remains unchanged for now (focuses on data export)...

func scriptTable(db *sql.DB, tableName string) (string, error) {
	var buffer strings.Builder

	// 1. Tables & Columns
	createStmt, err := getCreateTableStatement(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error creating table statement for %s: %v", tableName, err)
	}
	buffer.WriteString(createStmt + "\n\n")

	// 2. Sequences
	seqStmts, err := scriptSequences(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting sequences for %s: %v", tableName, err)
	}
	buffer.WriteString(seqStmts + "\n\n")

	// 3. Constraints (Primary, Unique, Check)
	constraints, err := scriptConstraints(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting constraints for %s: %v", tableName, err)
	}
	buffer.WriteString(constraints + "\n\n")

	// 4. Indexes (non-PK)
	idxStmt, err := scriptIndexes(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting indexes for %s: %v", tableName, err)
	}
	buffer.WriteString(idxStmt + "\n\n")

	// 5. Triggers
	triggers, err := scriptTriggers(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting triggers for %s: %v", tableName, err)
	}
	buffer.WriteString(triggers + "\n\n")

	// 6. Table Data
	copyStmt, err := getTableDataCopyFormat(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error generating COPY statement for %s: %v", tableName, err)
	}
	buffer.WriteString(copyStmt + "\n\n")

	return buffer.String(), nil
}

// Existing Functions (Updated or Unchanged)
func scriptSequences(db *sql.DB, tableName string) (string, error) {
	var sequencesSQL strings.Builder
	query := `
SELECT 'CREATE SEQUENCE ' || n.nspname || '.' || c.relname || ';' as seq_creation,
       'ALTER TABLE ' || quote_ident(n.nspname) || '.' || quote_ident(t.relname) ||
       ' ALTER COLUMN ' || quote_ident(a.attname) ||
       ' SET DEFAULT nextval(''' || n.nspname || '.' || c.relname || '''::regclass);' as col_default
FROM pg_class c
JOIN pg_namespace n ON c.relnamespace = n.oid
JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a' AND d.classid = 'pg_class'::regclass
JOIN pg_attrdef ad ON ad.adrelid = d.refobjid AND ad.adnum = d.refobjsubid
JOIN pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid
JOIN pg_class t ON t.oid = d.refobjid AND t.relkind = 'r'
WHERE c.relkind = 'S' AND t.relname = $1 AND n.nspname = 'public';
`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying sequences: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var seqCreation, colDefault string
		if err := rows.Scan(&seqCreation, &colDefault); err != nil {
			return "", err
		}
		sequencesSQL.WriteString(seqCreation + "\n" + colDefault + "\n")
	}
	return sequencesSQL.String(), nil
}

func scriptIndexes(db *sql.DB, tableName string) (string, error) {
	var indexesSQL strings.Builder
	query := `
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = $1 AND schemaname = 'public'
AND indexname NOT LIKE '%_pkey';
`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying indexes: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var indexName, indexDef string
		if err := rows.Scan(&indexName, &indexDef); err != nil {
			return "", err
		}
		indexesSQL.WriteString(indexDef + ";\n")
	}
	return indexesSQL.String(), nil
}

// New Functions for Missing Components
func scriptConstraints(db *sql.DB, tableName string) (string, error) {
	var constraintsSQL strings.Builder
	query := `
SELECT con.conname AS constraint_name,
       pg_get_constraintdef(con.oid) AS constraint_def,
       con.contype
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = connamespace
WHERE rel.relname = $1 AND nsp.nspname = 'public'
AND con.contype IN ('p', 'u', 'c', 'f');
`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying constraints: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var constraintName, constraintDef, conType string
		if err := rows.Scan(&constraintName, &constraintDef, &conType); err != nil {
			return "", err
		}
		// Include Primary (p), Unique (u), Check (c), and Foreign (f) constraints
		constraintsSQL.WriteString(fmt.Sprintf("ALTER TABLE public.%s ADD CONSTRAINT %s %s;\n", tableName, constraintName, constraintDef))
	}
	return constraintsSQL.String(), nil
}

func scriptTriggers(db *sql.DB, tableName string) (string, error) {
	var triggersSQL strings.Builder
	query := `
SELECT pg_get_triggerdef(t.oid) AS trigger_def
FROM pg_trigger t
JOIN pg_class rel ON rel.oid = t.tgrelid
JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
WHERE rel.relname = $1 AND nsp.nspname = 'public';
`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying triggers: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var triggerDef string
		if err := rows.Scan(&triggerDef); err != nil {
			return "", err
		}
		triggersSQL.WriteString(triggerDef + ";\n")
	}
	return triggersSQL.String(), nil
}

func scriptViews(db *sql.DB) (string, error) {
	var viewsSQL strings.Builder
	query := `
SELECT 'CREATE VIEW ' || n.nspname || '.' || c.relname || ' AS ' || pg_get_viewdef(c.oid) || ';' AS view_def
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'v' AND n.nspname = 'public';
`
	rows, err := db.Query(query)
	if err != nil {
		return "", fmt.Errorf("error querying views: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var viewDef string
		if err := rows.Scan(&viewDef); err != nil {
			return "", err
		}
		viewsSQL.WriteString(viewDef + "\n")
	}
	return viewsSQL.String(), nil
}

func scriptFunctionsAndProcedures(db *sql.DB) (string, error) {
	var funcsSQL strings.Builder
	query := `
SELECT pg_get_functiondef(p.oid) AS func_def
FROM pg_proc p
JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'public';
`
	rows, err := db.Query(query)
	if err != nil {
		return "", fmt.Errorf("error querying functions and procedures: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var funcDef string
		if err := rows.Scan(&funcDef); err != nil {
			return "", err
		}
		funcsSQL.WriteString(funcDef + "\n\n")
	}
	return funcsSQL.String(), nil
}

