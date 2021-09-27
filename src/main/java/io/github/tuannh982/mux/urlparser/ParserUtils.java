package io.github.tuannh982.mux.urlparser;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParserUtils {
    public static boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("JDBC connection string is null");
        }
        return url.startsWith(Constants.URL_PREFIX);
    }

    public static ParsedUrl parse(String url, Properties info) throws SQLException {
        if (info == null) {
            info = new Properties();
        }
        if (acceptsURL(url)) {
            Matcher matcher = Constants.URL_PATTERN.matcher(url);
            if (matcher.find()) {
                String configServerAddresses = matcher.group(1);
                if (StringUtils.isBlank(configServerAddresses)) {
                    throw new SQLException("configServerAddresses is blank");
                }
                String configKeyId = matcher.group(2);
                if (StringUtils.isBlank(configKeyId)) {
                    throw new SQLException("configKeyId is blank");
                }
                String database = matcher.group(3);
                if (StringUtils.isBlank(database)) {
                    throw new SQLException("database is blank");
                }
                String dbParams = matcher.group(4);
                Properties properties;
                if (StringUtils.isBlank(dbParams) || dbParams.charAt(0) != '?') {
                    properties = info;
                } else {
                    properties = parseDBProperties(dbParams.substring(1), info);
                }
                return new ParsedUrl(
                        configServerAddresses.split(","),
                        configKeyId,
                        database,
                        properties
                );
            } else {
                return null;
            }
        } else {
            throw new SQLException("JDBC connection string is not accepted");
        }
    }

    private static Properties parseDBProperties(String dBProperties, Properties info) {
        if (!StringUtils.isBlank(dBProperties)) {
            String[] params = dBProperties.split("&");
            for (String parameter : params) {
                int pos = parameter.indexOf('=');
                if (pos == -1) {
                    if (!info.containsKey(parameter)) {
                        info.setProperty(parameter, "");
                    }
                } else if (!info.containsKey(parameter.substring(0, pos))) {
                    info.setProperty(parameter.substring(0, pos), parameter.substring(pos + 1));
                }
            }
        }
        return info;
    }
}
