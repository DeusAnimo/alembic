import logging

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import CreateColumn

from .base import ColumnDefault, DropColumn
from .base import format_server_default, add_column, alter_table, drop_column
from .impl import DefaultImpl

log = logging.getLogger(__name__)


class VerticaImpl(DefaultImpl):
    __dialect__ = "vertica"
    transactional_ddl = True
    batch_separator = "$"
    type_synonyms = DefaultImpl.type_synonyms + (
        {"VARCHAR", "VARCHAR2", "CHAR", "TEXT", "LONG VARCHAR"},
        {"INTEGER", "INT", "INT8", "SMALLINT", "TINYINT"},
        {"BINARY", "VARBINARY", "LONG VARBINARY", "BYTEA", "RAW"},
        {"FLOAT", "FLOAT8", "DOUBLE", "REAL"},
    )

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
                self._exec(
                    f"ALTER TABLE {table_name} ADD COLUMN {column_name}_temp {type_}; "
                    f"ALTER TABLE {table_name} ALTER COLUMN {column_name}_temp DROP DEFAULT; SELECT MAKE_AHM_NOW();"
                    f"ALTER TABLE {table_name} DROP COLUMN {column_name} CASCADE;"
                    f"ALTER TABLE {table_name} RENAME COLUMN {column_name}_temp to {column_name};"
                )
                if existing_nullable is not None and not existing_nullable:
                    self._exec(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET NOT NULL")

            else:
                self._exec(
                    f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET DATA TYPE {type_}"
                )
                if nullable is not None and not nullable:
                    self._exec(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} SET NOT NULL")

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

    def create_index(self, index):
        if index.unique is not None:
            existTable = self.dialect.identifier_preparer.format_table(index.table)
            for column in index.columns:
                if index.unique:
                    self._exec(f"ALTER TABLE {existTable} "
                               f"ADD UNIQUE ({column.name})")

    def drop_index(self, index):
        pass


@compiles(DropColumn)
def visit_drop_column(element, compiler, **kw):
    return "%s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        drop_column(compiler, element.column.name, **kw),
        "CASCADE"
    )


@compiles(ColumnDefault, "vertica")
def visit_column_default(element, compiler, **kw):
    return "%s %s %s" % (
        alter_table(compiler, element.table_name, element.schema),
        add_column(compiler, element.column),
        "DEFAULT %s" % format_server_default(compiler, element.default)
        if element.default is not None
        else "DEFAULT NULL",
    )


@compiles(CreateColumn, 'vertica')
def use_identity(element, compiler, **kw):
    text = compiler.visit_create_column(element, **kw)
    text = text.replace("SERIAL", "IDENTITY(1,1)")
    return text
