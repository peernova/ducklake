package io.ducklake.service.parser;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * Represents a table reference extracted from SQL.
 * catalog.schema.table or schema.table or just table
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TableReference {
    private String catalog;
    private String schema;
    private String table;
    private String alias;
    private ReferenceType type;

    public enum ReferenceType {
        SELECT,    // Table being read from
        INSERT,    // Table being inserted into
        UPDATE,    // Table being updated
        DELETE,    // Table being deleted from
        JOIN,      // Table in a join
        SUBQUERY   // Table in subquery
    }

    /**
     * Returns fully qualified name: catalog.schema.table
     */
    public String getFullyQualifiedName() {
        StringBuilder sb = new StringBuilder();
        if (catalog != null) {
            sb.append(catalog).append(".");
        }
        if (schema != null) {
            sb.append(schema).append(".");
        }
        sb.append(table);
        return sb.toString();
    }

    /**
     * Check if this reference matches a pattern (for RLS)
     */
    public boolean matches(String catalogPattern, String schemaPattern, String tablePattern) {
        return (catalogPattern == null || catalogPattern.equals("*") || catalogPattern.equalsIgnoreCase(catalog))
            && (schemaPattern == null || schemaPattern.equals("*") || schemaPattern.equalsIgnoreCase(schema))
            && (tablePattern == null || tablePattern.equals("*") || tablePattern.equalsIgnoreCase(table));
    }
}
