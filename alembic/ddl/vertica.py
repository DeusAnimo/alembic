import logging

from sqla_vertica_python.vertica_python import VerticaDialect
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import reflection
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateColumn

from .base import DropColumn
from .base import alter_table, drop_column
from .impl import DefaultImpl

log = logging.getLogger(__name__)


class VerticaImpl(DefaultImpl, VerticaDialect):
    __dialect__ = "vertica"
    transactional_ddl = True
    batch_separator = "$"
    type_synonyms = DefaultImpl.type_synonyms + (
        {"VARCHAR", "VARCHAR2", "CHAR", "TEXT", "LONG VARCHAR"},
        {"INTEGER", "INT", "INT8", "SMALLINT", "TINYINT"},
        {"BINARY", "VARBINARY", "LONG VARBINARY", "BYTEA", "RAW"},
        {"FLOAT", "FLOAT8", "DOUBLE", "REAL"},
    )
    # update uuid column type from PostgreSQL
    VerticaDialect.ischema_names.update({'UUID': UUID})

    def __init__(self, *arg, **kw):
        super(VerticaImpl, self).__init__(*arg, **kw)
        self.batch_separator = self.context_opts.get(
            "vertica_batch_separator", self.batch_separator
        )

    def _exec(self, construct, *args, **kw):
        result = super(VerticaImpl, self)._exec(construct, *args, **kw)
        if self.as_sql and self.batch_separator:
            self.static_output(self.batch_separator)
        return result

    @reflection.cache
    def _get_unique_constraints(self, table_name, schema=None, **kw):
        query = "SELECT constraint_id, constraint_name, column_name FROM v_catalog.constraint_columns \n\
                 WHERE table_name = '" + table_name + "'"
        if schema is not None:
             query += " AND table_schema = '" + schema + "'"
        query += " AND constraint_type = 'u'"
        res = self._exec(query).fetchall()
        result = [{"name": unique, "column_name": column} for _, unique, column in res]
        return result

    def _get_columns_info(self, column_name, table_name, schema=None, **kw):
        init_vertica = VerticaDialect()
        all_columns_table = init_vertica.get_columns(
            connection=self.bind, table_name=table_name, schema=schema, **kw
        )
        if column_name:
            column_info = [column for column in all_columns_table if column["name"] == column_name]
            return column_info[0]
        return all_columns_table

    def alter_column(
            self,
            table_name,
            column_name,
            nullable=None,
            server_default=False,
            name=None,
            type_=None,
            schema=None,
            autoincrement=None,
            existing_type=None,
            existing_server_default=None,
            existing_nullable=None,
            existing_autoincrement=None,
            **kw
    ):
        using = kw.pop("postgresql_using", None)

        if type_ is not None:
            if existing_type is not None:
                query = f"ALTER TABLE {table_name} ADD COLUMN {column_name}_temp {type_}"
                if existing_nullable is not None:
                    query += " NULL;" if existing_nullable else " NOT NULL;"
                query += f"ALTER TABLE {table_name} ALTER COLUMN {column_name}_temp DROP DEFAULT; " \
                         f"SELECT MAKE_AHM_NOW(); ALTER TABLE {table_name} DROP COLUMN {column_name} CASCADE;" \
                         f"ALTER TABLE {table_name} RENAME COLUMN {column_name}_temp to {column_name};"
                self._exec(query)
            else:
                self._exec(
                    f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET DATA TYPE {type_}"
                )

        super(VerticaImpl, self).alter_column(
            table_name,
            column_name,
            nullable=nullable,
            server_default=server_default,
            name=name,
            schema=schema,
            autoincrement=autoincrement,
            existing_type=existing_type,
            existing_server_default=existing_server_default,
            existing_nullable=existing_nullable,
            existing_autoincrement=existing_autoincrement,
            **kw
        )

    # Disable index creation since that's not a thing in Vertica
    def create_index(self, index):
        pass

    def drop_index(self, index):
        pass


@compiles(DropColumn)
def visit_drop_column(element, compiler, **kw):
    return "%s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        drop_column(compiler, element.column.name, **kw),
        "CASCADE"
    )


@compiles(CreateColumn, 'vertica')
def use_identity(element, compiler, **kw):
    text = compiler.visit_create_column(element, **kw)
    text = text.replace("SERIAL", "IDENTITY(1,1)")
    return text
