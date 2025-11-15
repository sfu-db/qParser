package com.parseval.schema;

import java.util.List;

public record ForeignKeySpec(String name,                     // may be null
                             List<String> referencingColumns,
                             String referencedTable,
                             List<String> referencedColumns) {
}
