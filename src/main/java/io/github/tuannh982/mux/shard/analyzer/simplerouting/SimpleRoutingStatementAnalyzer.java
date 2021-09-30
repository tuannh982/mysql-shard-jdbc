package io.github.tuannh982.mux.shard.analyzer.simplerouting;

import io.github.tuannh982.mux.shard.shardops.ShardOps;
import net.sf.jsqlparser.statement.Statement;

import java.util.Map;
import java.util.Set;

public class SimpleRoutingStatementAnalyzer {
    private final String schema;
    private final Statement statement;
    private final ShardOps shardOps;
    private final SimpleRoutingStatementVisitor visitor;

    public SimpleRoutingStatementAnalyzer(String schema, Statement statement, ShardOps shardOps) {
        this.schema = schema;
        this.statement = statement;
        this.shardOps = shardOps;
        this.visitor = new SimpleRoutingStatementVisitor(this);
    }

    public void analyze() {
        statement.accept(visitor);
    }

    public void fillingParameters(Map<Integer,byte[]> valueMap) {
        // TODO
    }

    public boolean containsJoin() {
        // TODO
    }

    public boolean containsSubQuery() {
        // TODO
    }

    public Set<Integer> extractInvolvedShards() {
        // TODO
    }
}
