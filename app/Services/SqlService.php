<?php namespace App\Services;

use Exception;
use PDO;
use PDOException;
use PDOStatement;

/**
 * Description of MysqlDb
 *
 * @return bool false on failure / PDD object instance on success
 * @author julio.rivas
 * Connect to the database
 *
 */
class SqlService
{
    /**
     * @var
     */
    protected $connection;

    protected $selectStatement = null;

    protected $updateStatement = null;

    protected $insertStatement = null;

    protected $deleteStatement = null;

    protected $queries = null;

    protected $config = array();

    /**
     * MysqlDb constructor.
     * @param $config
     * @throws Exception
     */
    function __construct( $config )
    {
        $this->config = $config;

        $this->connection = $this->PDOConnect();
    }

    /**
     * Query the database
     *
     * @param string $query
     * @param array $bindings
     *
     * @return mixed The result of the mysqli::query() function
     *
     * @throws Exception
     */

    public function update( ?string $query = null, $bindings = array() ): void
    {
        if ( !empty( $query ) ) {
            $this->updateStatement = $query;
        }

        $sth = $this->connection->prepare( $this->updateStatement );

        $sth->execute( $bindings );

        $this->validateExecution( $sth, $bindings, $query, 'updateStatement' );

        $sth->closeCursor();

        unset( $sth );
    }

    /**
     * @param null $query
     * @param array $bindings
     * @throws Exception
     */
    public function insert( $query = null, $bindings = array() ): void
    {
        if ( !empty( $query ) ) {
            $this->insertStatement = $query;
        }

        $sth = $this->connection->prepare( $this->insertStatement );

        $sth->execute( $bindings );

        $this->validateExecution( $sth, $bindings, $query, 'insertStatement' );

        $sth->closeCursor();

        unset( $sth );
    }

    /**
     * @param null $query
     * @param array $bindings
     * @throws Exception
     */
    public function delete( $query = null, $bindings = array() ): void
    {
        if ( !empty( $query ) ) {
            $this->deleteStatement = $query;
        }

        $sth = $this->connection->prepare( $this->deleteStatement );

        $sth->execute( $bindings );

        $this->validateExecution( $sth, $bindings, $query, 'deleteStatement' );

        $sth->closeCursor();

        unset( $sth );
    }

    /**
     * @param $queryName
     * @param array $bindings
     * @throws Exception
     */
    public function executeRegisteredQuery( $queryName, $bindings = array() ): void
    {
        if ( empty( $this->queries[ $queryName ][ 'string_query' ] ) === false ) {
            $sth = $this->connection->prepare( $this->queries[ $queryName ][ 'string_query' ] );

            $sth->execute( $bindings );

            $this->validateExecution( $sth, $bindings, null, null, $queryName );

            $sth->closeCursor();

            unset( $sth );
        } else {
            throw new Exception( "query $queryName is not registered" );
        }
    }

    /**
     * Fetch rows from the database (SELECT query)
     *
     * @param string|null $query The query string
     * @param array $bindings
     * @param bool $fetchAssoc
     * @return array False on failure / array Database rows on success
     * @throws Exception
     */

    public function select( ?string $query = null, array $bindings = array(), bool $fetchAssoc = true ): array
    {
        $pdo = $this->connection;

        if ( empty( $query ) === false ) {
            $sth = $pdo->prepare( $query );
        } else {
            $sth = $this->selectStatement;
        }

        $sth->execute( $bindings );

        $result = $this->validateExecution( $sth, $bindings, $query, 'selectStatement', null, $fetchAssoc );

        $sth->closeCursor();

        unset( $sth );

        return $result;
    }

    /**
     * @param $query
     * @param $path
     * @param bool $csvFile
     *
     * @throws Exception
     */

    public function PGdumpQueryToCsv( $query, $path, $csvFile = false )
    {
        $convertToCsv = "";

        if ( $csvFile ) {
            $convertToCsv = " With CSV DELIMITER ','";
        }

        $command = "PGPASSWORD=" . $this->config[ 'password' ] . " psql --host=\"" . $this->config[ 'host' ] . "\" --port=5432 --username \"" . $this->config[ 'userName' ] . "\" --dbname=\"" . $this->config[ 'DBName' ] . "\" --echo-queries --command=\"\\copy (" . $query . ") TO '" . $path . "'$convertToCsv\"";
        exec( $command, $output, $status );

        if ( $status !== 0 ) {
            throw new Exception ( "file load failed" );
        }
    }

    /**
     * import a CSV file into a PG table, performing a truncate table (optional).
     *
     * @param string $tableName
     * @param string $csvFilePath
     * @param bool $truncateTable
     *
     * @throws Exception
     */

    public function PGdumpCsvToTable( string $tableName, string $csvFilePath, bool $truncateTable = true ): void
    {
        if ( $truncateTable === true ) {
            $this->insert( "truncate table $tableName" );
        }

        $command = "PGPASSWORD=" . $this->config[ 'password' ] . " psql --host=\"" . $this->config[ 'host' ] . "\" --port=5432 --username \"" . $this->config[ 'userName' ] . "\" --dbname=\"" . $this->config[ 'DBName' ] . "\" --echo-queries --command=\"\\copy $tableName FROM '" . $csvFilePath . "' csv\"";

        exec( $command, $output, $status );

        if ( $status !== 0 ) {
            throw new Exception ( "file load failed" );
        }
    }

    /**
     * @param $tableName
     * @return array
     * @throws Exception
     */
    public function schemaToBigQueryArray( $tableName )
    {
        $result = $this->select( "show COLUMNS from " . $tableName );

        $fields = array();

        foreach ( $result as $key => $column ) {
            $fields[ $key ][ 'name' ] = $column[ 'Field' ];

            if ( strpos( $column[ "Type" ], "char" ) !== false || strpos( $column[ "Type" ], "text" ) !== false || strpos( $column[ "Type" ], "string" ) !== false ) {
                $fields[ $key ][ 'type' ] = "string";
            } else if ( strpos( $column[ "Type" ], "int" ) !== false ) {
                $fields[ $key ][ 'type' ] = "integer";
            } else if ( strpos( $column[ "Type" ], "double" ) !== false || strpos( $column[ "Type" ], "float" ) !== false || strpos( $column[ "Type" ], "decimal" ) !== false ) {
                $fields[ $key ][ 'type' ] = "float";
            } else if ( strpos( $column[ "Type" ], "boolean" ) !== false ) {
                $fields[ $key ][ 'type' ] = "boolean";
            } else if ( strpos( $column[ "Type" ], "timestamp" ) !== false ) {
                $fields[ $key ][ 'type' ] = "timestamp";
            } else if ( strpos( $column[ "Type" ], "datetime" ) !== false ) {
                $fields[ $key ][ 'type' ] = "datetime";
            } else if ( strpos( $column[ "Type" ], "date" ) !== false ) {
                $fields[ $key ][ 'type' ] = "date";
            } else if ( strpos( $column[ "Field" ], "time" ) !== false ) {
                $fields[ $key ][ 'type' ] = "time";
            }
        }

        return $fields;
    }

    public function importFromCsvFile( $tableName, $csvFilePath, $truncateTable = true )
    {
        if ( $truncateTable ) {
            $truncateCommand = 'mysql -u ' . $this->config[ 'userName' ] . ' -p' . $this->config[ 'password' ] . ' ' . $this->config[ 'DBName' ] . ' -e "truncate table ' . $this->config[ 'DBName' ] . '.' . $tableName . '"';
            exec( $truncateCommand, $output, $status );
            unset( $output );
        }

        $cmdMySQLImport = "mysqlimport -u " . $this->config[ 'userName' ] . " -p" . $this->config[ 'password' ] . " --local --fields-terminated-by ',' --fields-optionally-enclosed-by '" . '"' . "' " . $this->config[ 'DBName' ] . " " . $csvFilePath;
        exec( $cmdMySQLImport, $output, $status );
        unset( $output );
    }

    /**
     * @param $PGSchemaName
     * @param $tableName
     * @return mixed
     * @throws Exception
     */
    public function getBigquerySchemaFromPGTable( $PGSchemaName, $tableName )
    {
        $pgSchema = $this->getFields( $PGSchemaName, $tableName );

        $schemaArray = array();

        foreach ( $pgSchema as $field ) {
            if ( stripos( strtolower( $field[ 'type' ] ), "char" ) !== false || stripos( strtolower( $field[ 'type' ] ), "text" ) !== false || stripos( strtolower( $field[ 'type' ] ), "string" ) !== false || stripos( strtolower( $field[ 'type' ] ), "GEOMETRY" ) !== false ) {
                $schemaArray[ $field[ 'name' ] ] = "string";
            } else if ( stripos( strtolower( $field[ 'type' ] ), "int" ) !== false ) {
                $schemaArray[ $field[ 'name' ] ] = "integer";
            } else if ( stripos( strtolower( $field[ 'type' ] ), "double" ) !== false || stripos( $field[ 'type' ], "float" ) !== false || stripos( $field[ 'type' ], "decimal" ) !== false ) {
                $schemaArray[ $field[ 'name' ] ] = "float";
            } else if ( stripos( strtolower( $field[ 'type' ] ), "boolean" ) !== false ) {
                $schemaArray[ $field[ 'name' ] ] = "boolean";
            } else if ( stripos( strtolower( $field[ 'type' ] ), "timestamp" ) !== false ) {
                $schemaArray[ $field[ 'name' ] ] = "timestamp";
            } else if ( stripos( strtolower( $field[ 'type' ] ), "datetime" ) !== false ) {
                $schemaArray[ $field[ 'name' ] ] = "datetime";
            } else if ( stripos( strtolower( $field[ 'type' ] ), "date" ) !== false ) {
                $schemaArray[ $field[ 'name' ] ] = "date";
            } else if ( stripos( strtolower( $field[ 'type' ] ), "time" ) !== false ) {
                $schemaArray[ $field[ 'name' ] ] = "time";
            }
        }

        $schemaString = '[';

        foreach ( $schemaArray as $name => $type ) {
            $schemaString .= '{"name":"' . $name . '","type":"' . $type . '","mode":"NULLABLE"},';
        }

        $schemaString = substr( $schemaString, 0, -1 );

        $schemaString .= ']';

        return json_decode( $schemaString, true );
    }

    /**
     * create a PostgresSQL Table from a BigQuery schema Table
     *
     * @param string $PGSchemaName desire PostgresSQL schema Name
     * @param string $PGTableName desire PostgresSQL table Name
     * @param array $BQSchema BigQuery Table Schema
     *
     * @throws    Exception
     */

    public function createTableFromBigQuerySchema( string $PGSchemaName, string $PGTableName, array $BQSchema )
    {
        $query = /** @lang PG */
            "CREATE TABLE IF NOT EXISTS $PGSchemaName.$PGTableName ( ";

        $fields = '';

        foreach ( $BQSchema as $field ) {
            $fields .= $field[ 'name' ] . ' ' . $this->getPGFieldType( $field[ 'type' ] ) . ',';
        }

        $fields = substr( $fields, 0, -1 );

        $query .= $fields . ')';

        $this->RegisterQueryStatement( 'create', $query );

        $this->executeRegisteredQuery( 'create' );
    }

    /**
     * @param $schema
     * @param $tableName
     * @return array
     * @throws Exception
     */
    public function getFields( $schema, $tableName )
    {

        return $this->select( /** @lang PG */ "SELECT
        INF.rubrique_ordre,
        INF.rubrique_name as name,
        -- INF.rubrique_type_inf,
        -- CAT.rubrique_type_cat,
        CASE
                WHEN CAT.rubrique_type_cat LIKE'GEOMETRY(%'
                        AND parenthese_1 > 0
                        AND virgule_1 > 0
                        THEN SUBSTRING(CAT.rubrique_type_cat FROM (parenthese_1 + 1) FOR (virgule_1 - parenthese_1 - 1) )
                ELSE
                        INF.rubrique_type_inf
        END												AS type

    FROM
        (
                -- *****************************************
                -- Rubriques avec un type 'en clair'
                -- *****************************************
                SELECT
                        ordinal_position						AS rubrique_ordre ,
                        column_name								AS rubrique_name ,
                        CASE
                                WHEN UPPER(data_type) IN('CHARACTER VARYING', 'TEXT')
                                        THEN 'STRING'
                                ELSE
                                        UPPER(data_type)
                        END										AS rubrique_type_inf

                FROM
                        information_schema.columns AS c

                WHERE
                        -- database
                        table_catalog = ?
                        -- schema
                        AND table_schema = ?
                        -- table
                        AND table_name = ?

                ORDER BY
                        c.ordinal_position
        ) INF

        LEFT JOIN
        (
                SELECT
                        SRC.*,
                        POSITION('(' IN rubrique_type_cat)		AS parenthese_1,
                        POSITION(',' IN rubrique_type_cat)		AS virgule_1
                FROM
                (
                -- *****************************************
                -- Pour les champs USER-DEFINED
                -- *****************************************
                        SELECT
                                a.attname							AS rubrique_name ,
                                REPLACE(UPPER(pg_catalog.format_type(a.atttypid, a.atttypmod)), 'PUBLIC.', '') AS rubrique_type_cat

                        FROM
                                pg_catalog.pg_attribute a

                        WHERE
                                -- Pour n'avoir que
                                -- les colonnes non system
                                a.attnum > 0
                                -- non supprimées
                                AND NOT a.attisdropped
                                -- OID de la table
                                AND a.attrelid
                                IN (
                                        SELECT c.oid
                                        FROM
                                                pg_catalog.pg_namespace n
                                                INNER JOIN pg_catalog.pg_class c ON n.oid = c.relnamespace
                                        WHERE
                                                -- pg_catalog.pg_table_is_visible(c.oid)
                                                -- AND
                                                c.relkind IN ('r','v')
                                                AND
                                                -- schema
                                                n.nspname = ?
                                                AND
                                                -- table
                                                c.relname = ?
                                )
                        ) SRC
        )CAT
                ON INF.rubrique_name = CAT.rubrique_name
    ;", [ $this->config[ 'DBName' ], $schema, $tableName, $schema, $tableName ] );


    }

    /**
     * @return PDO
     * @throws Exception
     */
    private function PDOConnect(): PDO
    {
        $new_queries = array();

        unset( $this->connection );

        $dns = $this->config[ 'DBMS' ] . ':' . $this->config[ 'DBName' ];

        try {
            $connection = new PDO( $dns );
        } catch ( PDOException $e ) {
            throw new Exception( $e->getMessage() );
        }

        if ( empty( $this->queries ) === false ) {
            foreach ( $this->queries as $queryName => $query ) {
                $new_queries[ $queryName ][ 'string_query' ] = $query[ 'string_query' ];
            }

            unset( $this->queries );

            $this->queries = $new_queries;
        }

        return $connection;
    }

    /**
     * @param PDOStatement $sth
     * @param null $bindings
     * @param null $query
     * @param null $propertyName
     * @param null $queryName
     * @param bool|null $fetchAssoc
     *
     * @return array|null
     *
     * @throws Exception
     */

    private function validateExecution( PDOStatement $sth, $bindings = null, $query = null, $propertyName = null, $queryName = null, ?bool $fetchAssoc = null ): ?array
    {
        if ( !empty( $sth->errorInfo()[ 1 ] ) || !empty( $sth->errorInfo()[ 2 ] ) ) {
            $reconnectError = $sth->errorInfo()[ 1 ] . " " . $sth->errorInfo()[ 2 ];

            if ( $reconnectError === '7 SSL connection has been closed unexpectedly' || $reconnectError === '7 SSL SYSCALL error: EOF detected' ) {
                echo "reconnecting SQL Server" . PHP_EOL;

                $sth->closeCursor();

                sleep( 2 );

                unset( $sth );

                $this->connection = $this->PDOConnect();

                if ( !empty( $queryName ) ) {
                    $sth = $this->connection->prepare( $this->queries[ $queryName ][ 'string_query' ] );

                    $sth->execute( $bindings );
                } else if ( !empty( $query ) ) {
                    $sth = $this->connection->prepare( $query );

                    $this->$propertyName = $query;

                    $sth->execute( $bindings );
                } else {
                    $sth = $this->connection->prepare( $this->$propertyName );

                    $sth->execute( $bindings );
                }

                if ( !empty( $sth->errorInfo()[ 1 ] ) || !empty( $sth->errorInfo()[ 2 ] ) ) {
                    $sth->closeCursor();

                    throw new Exception ( $sth->errorInfo()[ 1 ] . " " . $sth->errorInfo()[ 2 ] );
                }
            } else {
                $sth->closeCursor();

                throw new Exception ( $sth->errorInfo()[ 1 ] . " " . $sth->errorInfo()[ 2 ] );
            }
        }

        $result = null;

        if ( $propertyName === 'selectStatement' ) {
            $result = $fetchAssoc === true ? $sth->fetchAll( PDO::FETCH_ASSOC ) : $sth->fetchAll( PDO::FETCH_NUM );
        }

        $sth->closeCursor();

        unset( $sth );

        return $result;
    }

    public function setSelectStatement( $selectStatement )
    {
        $this->selectStatement = $selectStatement;
    }

    public function setUpdateStatement( $updateStatement )
    {
        $this->updateStatement = $updateStatement;
    }

    public function setInsertStatement( $insertStatement )
    {
        $this->insertStatement = $insertStatement;
    }

    public function RegisterQueryStatement( $queryName, $query )
    {
        $this->queries[ $queryName ][ 'string_query' ] = $query;
    }

    /**
     * @param $schema
     * @param $table_name
     * @param $keys
     * @param $metrics
     * @return string
     * @throws Exception
     */
    public function createPgTableWithPrimaryKeys( $schema, $table_name, $keys, $metrics )
    {
        $fieldsDefinitions = "";
        $dimensions = "";
        $metricsFields = "";
        $excludedMetrics = "";

        $query = /** @lang PG */
            "CREATE TABLE IF NOT EXISTS $schema.$table_name (";

        foreach ( $keys as $fieldName => $type ) {
            $dimensions .= $fieldName . ",";
            $fieldsDefinitions .= $fieldName . " " . $type . " NOT NULL,";
        }

        foreach ( $metrics as $fieldName => $type ) {
            $metricsFields .= $fieldName . ",";
            $excludedMetrics .= $fieldName . " = EXCLUDED." . $fieldName . ",";
            $fieldsDefinitions .= $fieldName . " " . $type . ",";
        }

        $placeholders = str_repeat( "?,", count( $keys ) + count( $metrics ) );

        $excludedMetrics = substr( $excludedMetrics, 0, -1 );
        $placeholders = substr( $placeholders, 0, -1 );
        $dimensions = substr( $dimensions, 0, -1 );
        $metricsFields = substr( $metricsFields, 0, -1 );
        $fieldsDefinitions = substr( $fieldsDefinitions, 0, -1 );

        $query .= $fieldsDefinitions . ", CONSTRAINT " . $table_name . "_pkey PRIMARY KEY ($dimensions))";

        $insertStatement = /** @lang PG */
            "INSERT INTO $schema.$table_name ($dimensions,$metricsFields)
	VALUES ($placeholders)
	on conflict ($dimensions)
	do update set $excludedMetrics";

        $this->RegisterQueryStatement( "create_statement_internal_create", $query );
        $this->executeRegisteredQuery( "create_statement_internal_create" );

        return $insertStatement;
    }

    /**
     * retrieve PostgresSQL field type corresponding to a bigQuery field type
     *
     * @param string $BQFieldType
     *
     * @return string
     */

    private function getPGFieldType( string $BQFieldType ): string
    {
        if ( strtolower( $BQFieldType ) === 'integer' ) {
            return 'bigint';
        } else if ( strtolower( $BQFieldType ) === 'timestamp' ) {
            return 'timestamp without time zone';
        } else if ( strtolower( $BQFieldType ) === 'float' ) {
            return 'double precision';
        } else if ( strtolower( $BQFieldType ) === 'string' ) {
            return 'text COLLATE pg_catalog."default"';
        } else if ( strtolower( $BQFieldType ) === 'date' ) {
            return 'date';
        } else if ( strtolower( $BQFieldType ) === 'geography' ) {
            return 'text COLLATE pg_catalog."default"';
        } else if ( strtolower( $BQFieldType ) === 'boolean' ) {
            return 'boolean';
        } else {
            return 'string';
        }
    }

    /**
     * @param string $schema
     * @param string $table
     *
     * @return array
     *
     * @throws Exception
     */

    public function getFieldsAndTypeFromTable( string $schema, string $table ): array
    {
        $formatSchema = array();

        foreach ( $this->getFields( $schema, $table ) as $field ) {
            $formatSchema[ $field[ 'name' ] ] = $field[ 'type' ];
        }

        return $formatSchema;
    }

    public function close()
    {
        $this->connection = null;
        $this->insertStatement = null;
        $this->selectStatement = null;
        $this->updateStatement = null;
        $this->queries = null;

    }
}
