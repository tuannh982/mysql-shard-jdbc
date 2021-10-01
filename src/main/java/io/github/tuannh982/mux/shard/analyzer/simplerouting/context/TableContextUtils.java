package io.github.tuannh982.mux.shard.analyzer.simplerouting.context;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import net.sf.jsqlparser.schema.Table;
import org.apache.commons.lang3.StringUtils;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableContextUtils {
    public static String extractSchema(Table table, String schema) {
        return StringUtils.isBlank(table.getSchemaName()) ? schema : table.getSchemaName().toLowerCase();
    }

    public static String getFullTableName(Table table, String schema, boolean includeAlias) {
        StringBuilder ret = new StringBuilder();
        ret.append(StringUtils.isBlank(table.getSchemaName()) ? schema : table.getSchemaName().toLowerCase());
        ret.append('.');
        ret.append(table.getName().toLowerCase());
        if (includeAlias && table.getAlias() != null && !StringUtils.isBlank(table.getAlias().getName())) {
            ret.append('.');
            ret.append(table.getAlias().getName());
        }
        return ret.toString();
    }

    public static String getTableAlias(Table table) {
        if (table.getAlias() == null) {
            return null;
        } else {
            String alias = table.getAlias().getName();
            return StringUtils.isBlank(alias) ?  null : alias;
        }
    }
}
