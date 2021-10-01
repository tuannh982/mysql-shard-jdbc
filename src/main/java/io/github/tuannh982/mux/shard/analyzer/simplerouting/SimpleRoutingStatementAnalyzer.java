package io.github.tuannh982.mux.shard.analyzer.simplerouting;

import io.github.tuannh982.mux.shard.analyzer.simplerouting.context.FrameContext;
import io.github.tuannh982.mux.shard.analyzer.simplerouting.context.TableContext;
import io.github.tuannh982.mux.shard.shardops.ShardOps;
import lombok.AccessLevel;
import lombok.Getter;
import net.sf.jsqlparser.statement.Statement;

import java.sql.SQLException;
import java.util.*;

@Getter
public class SimpleRoutingStatementAnalyzer {
    private final Deque<FrameContext> stack;
    private final List<TableContext> tableContexts;
    private final String schema;
    private final Statement statement;
    private final ShardOps shardOps;
    @Getter(AccessLevel.NONE)
    private final SimpleRoutingStatementVisitor visitor;

    public SimpleRoutingStatementAnalyzer(String schema, Statement statement, ShardOps shardOps) {
        this.stack = new LinkedList<>();
        this.tableContexts = new ArrayList<>();
        this.schema = schema;
        this.statement = statement;
        this.shardOps = shardOps;
        this.visitor = new SimpleRoutingStatementVisitor(this);
    }

    public boolean containsJoin() {
        return visitor.containsJoin();
    }

    public boolean containsSubQuery() {
        return visitor.containsSubQuery();
    }

    public boolean usingValue() {
        return visitor.usingValue();
    }

    public void analyze() throws SQLException {
        try {
            statement.accept(visitor);
        } catch (SQLExceptionRTE e) {
            throw e.getInner();
        }
    }

    public int stackDepth() {
        return stack.size();
    }

    public void fillingParameters(Map<Integer,byte[]> valueMap) {
        // TODO
    }

    public Set<Integer> extractInvolvedShards() { /* must be called after analyze() */
        // TODO
    }
}
