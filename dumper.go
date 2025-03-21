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
		chunkSize = 1 // Avoid division by zero
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

	tables, err := getTables(db, opts)
	if err != nil {
		return err
	}

	var (
		wg sync.WaitGroup
		mx sync.Mutex
	)

	// Replace slices.Chunk with chunkSlice
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
	viewsSQL, err := scriptViews(db)
	if err != nil {
		return err
	}
	file.WriteString(viewsSQL)

	funcsSQL, err := scriptFunctions(db)
	if err != nil {
		return err
	}
	file.WriteString(funcsSQL)

	if err := writeFooter(file, info); err != nil {
		return err
	}

	return nil		
}

func (d *Dumper) DumpDBToCSV(outputDIR, outputFile string, opts *TableOptions) error {
	db, err := sql.Open("postgres", d.ConnectionString)
	if err != nil {
		return err
	}
	defer db.Close()

	file, err := os.Create(path.Join(outputDIR, outputFile))
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

	if err := writeFooter(file, info); err != nil {
		return err
	}

	tables, err := getTables(db, opts)
	if err != nil {
		return err
	}

	// Replace slices.Chunk with chunkSlice
	chunks := chunkSlice(tables, d.Parallels)
	g, _ := errgroup.WithContext(context.Background())
	for _, chunk := range chunks {
		g.SetLimit(len(chunk))
		for _, table := range chunk {
			table := table // capture the current value
			g.Go(func() error {
				records, err := getTableDataAsCSV(db, table)
				if err != nil {
					return err
				}

				f, err := os.Create(path.Join(outputDIR, table+".csv"))
				if err != nil {
					return err
				}
				defer f.Close()

				csvWriter := csv.NewWriter(f)
				if err := csvWriter.WriteAll(records); err != nil {
					return err
				}
				csvWriter.Flush()
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
	}
	return nil
}

func scriptTable(db *sql.DB, tableName string) (string, error) {
	var buffer string
	// Script CREATE TABLE statement
	createStmt, err := getCreateTableStatement(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error creating table statement for %s: %v", tableName, err)
	}
	buffer = buffer + createStmt + "\n\n"

	// Script associated sequences (if any)
	seqStmts, err := scriptSequences(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting sequences for table %s: %v", tableName, err)
	}
	buffer = buffer + seqStmts + "\n\n"

	// Script primary keys
	pkStmt, err := scriptPrimaryKeys(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting primary keys for table %s: %v", tableName, err)
	}
	buffer = buffer + pkStmt + "\n\n"

	// Script foreign keys
	fkStmt, err := scriptForeignKeys(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting foreign keys for table %s: %v", tableName, err)
	}
	buffer = buffer + fkStmt + "\n\n"
	
	// Script for indexes
	idxStmt, err := scriptIndexes(db, tableName)
	buffer = buffer + idxStmt + "\n\n"
	if err != nil {
		return "", fmt.Errorf("error scripting index keys for table %s: %v", tableName, err)
	}

	// Script for constraints
	constStmt, err := scriptConstraints(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting constraints for table %s: %v", tableName, err)
	}
	buffer += constStmt + "\n\n"

	// Script for triggers
	triggerStmt, err := scriptTriggers(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error scripting triggers for table %s: %v", tableName, err)
	}
	buffer += triggerStmt + "\n\n"

	// Dump table data
	copyStmt, err := getTableDataCopyFormat(db, tableName)
	if err != nil {
		return "", fmt.Errorf("error generating COPY statement for table %s: %v", tableName, err)
	}
	buffer = buffer + copyStmt + "\n\n"

	return buffer, nil
}

func scriptSequences(db *sql.DB, tableName string) (string, error) {
	var sequencesSQL strings.Builder

	// Query to identify sequences linked to the table's columns and fetch sequence definitions
	query := `
SELECT 'CREATE SEQUENCE ' || n.nspname || '.' || c.relname || ';' as seq_creation,
       pg_get_serial_sequence(quote_ident(n.nspname) || '.' || quote_ident(t.relname), quote_ident(a.attname)) as seq_owned,
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
		return "", fmt.Errorf("error querying sequences for table %s: %v", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var seqCreation, seqOwned, colDefault string
		if err := rows.Scan(&seqCreation, &seqOwned, &colDefault); err != nil {
			return "", fmt.Errorf("error scanning sequence information: %v", err)
		}

		// Here we directly use the sequence creation script.
		// The seqOwned might not be necessary if we're focusing on creation and default value setting.
		sequencesSQL.WriteString(seqCreation + "\n" + colDefault + "\n")
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating over sequences: %v", err)
	}

	return sequencesSQL.String(), nil
}

func scriptPrimaryKeys(db *sql.DB, tableName string) (string, error) {
	var pksSQL strings.Builder

	// Query to find primary key constraints for the specified table.
	query := `
SELECT con.conname AS constraint_name,
       pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = connamespace
WHERE con.contype = 'p' 
AND rel.relname = $1
AND nsp.nspname = 'public';
`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying primary keys for table %s: %v", tableName, err)
	}
	defer rows.Close()

	// Iterate through each primary key constraint found and script it.
	for rows.Next() {
		var constraintName, constraintDef string
		if err := rows.Scan(&constraintName, &constraintDef); err != nil {
			return "", fmt.Errorf("error scanning primary key information: %v", err)
		}

		// Construct the ALTER TABLE statement to add the primary key constraint.
		pksSQL.WriteString(fmt.Sprintf("ALTER TABLE public.%s ADD CONSTRAINT %s %s;\n",
			tableName, constraintName, constraintDef))
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating over primary keys: %v", err)
	}

	return pksSQL.String(), nil
}


func scriptForeignKeys(db *sql.DB, tableName string) (string, error) {
    var fksSQL strings.Builder
    query := `
SELECT con.conname AS constraint_name,
       pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = connamespace
WHERE con.contype = 'f' 
AND rel.relname = $1
AND nsp.nspname = 'public';
`
    rows, err := db.Query(query, tableName)
    if err != nil {
        return "", fmt.Errorf("error querying foreign keys: %v", err)
    }
    defer rows.Close()

    for rows.Next() {
        var constraintName, constraintDef string
        if err := rows.Scan(&constraintName, &constraintDef); err != nil {
            return "", err
        }
        fksSQL.WriteString(fmt.Sprintf("ALTER TABLE public.%s ADD CONSTRAINT %s %s;\n", tableName, constraintName, constraintDef))
    }
    return fksSQL.String(), nil
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

func scriptTriggers(db *sql.DB, tableName string) (string, error) {
	var triggersSQL strings.Builder

	query := `
SELECT tgname,
       pg_get_triggerdef(t.oid)
FROM pg_trigger t
JOIN pg_class rel ON rel.oid = t.tgrelid
JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
WHERE rel.relname = $1
AND nsp.nspname = 'public'
AND NOT t.tgisinternal;
`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying triggers for table %s: %v", tableName, err)
	}
	defer rows.Close()

	for rows.Next() {
		var triggerName, triggerDef string
		if err := rows.Scan(&triggerName, &triggerDef); err != nil {
			return "", err
		}
		triggersSQL.WriteString(triggerDef + ";\n")
	}
	return triggersSQL.String(), nil
}

func scriptConstraints(db *sql.DB, tableName string) (string, error) {
	var constraintsSQL strings.Builder
	query := `
SELECT con.conname AS constraint_name,
       con.contype,
       pg_get_constraintdef(con.oid) AS constraint_def
FROM pg_constraint con
JOIN pg_class rel ON rel.oid = con.conrelid
JOIN pg_namespace nsp ON nsp.oid = connamespace
WHERE con.contype IN ('u', 'c') 
AND rel.relname = $1
AND nsp.nspname = 'public';
`
	rows, err := db.Query(query, tableName)
	if err != nil {
		return "", fmt.Errorf("error querying constraints: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var constraintName, contype, constraintDef string
		if err := rows.Scan(&constraintName, &contype, &constraintDef); err != nil {
			return "", err
		}
		constraintsSQL.WriteString(fmt.Sprintf("ALTER TABLE public.%s ADD CONSTRAINT %s %s;\n", tableName, constraintName, constraintDef))
	}
	return constraintsSQL.String(), nil
}

func scriptViews(db *sql.DB) (string, error) {
	var viewsSQL strings.Builder

	query := `
SELECT table_name, view_definition
FROM information_schema.views
WHERE table_schema = 'public';
`
	rows, err := db.Query(query)
	if err != nil {
		return "", fmt.Errorf("error querying views: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var viewName, viewDef string
		if err := rows.Scan(&viewName, &viewDef); err != nil {
			return "", err
		}
		viewsSQL.WriteString(fmt.Sprintf("CREATE OR REPLACE VIEW public.%s AS %s;\n\n", viewName, viewDef))
	}
	return viewsSQL.String(), nil
}

func scriptFunctions(db *sql.DB) (string, error) {
	var functionsSQL strings.Builder

	query := `
SELECT proname,
       pg_get_functiondef(p.oid)
FROM pg_proc p
JOIN pg_namespace nsp ON p.pronamespace = nsp.oid
WHERE nsp.nspname = 'public';
`
	rows, err := db.Query(query)
	if err != nil {
		return "", fmt.Errorf("error querying functions: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var funcName, funcDef string
		if err := rows.Scan(&funcName, &funcDef); err != nil {
			return "", err
		}
		functionsSQL.WriteString(funcDef + "\n\n")
	}
	return functionsSQL.String(), nil
}

