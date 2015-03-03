package com.thinkaurelius.titan.diskstorage.accumulo;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.secure.SecurityToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;

/**
 *
 * @author edeprit
 */
public class AccumuloSecurityToken implements SecurityToken {

    private static final AccumuloSecurityToken INSTANCE;

    static {
        try {
            INSTANCE = new AccumuloSecurityToken();
        } catch (Exception e) {
            throw new RuntimeException("Error making Accumulo security token factory.");
        }
    }

    public static AccumuloSecurityToken getInstance() {
        return INSTANCE;
    }

    @Override
    public ReadToken newReadToken(Object... parameters) {
        Preconditions.checkArgument(parameters.length <= 1);
        if (parameters.length == 0) {
            return new ReadToken();
        } else {
            Preconditions.checkArgument(parameters[0] instanceof String);
            return new ReadToken((String) parameters[0]);
        }
    }

    @Override
    public WriteToken newWriteToken(Object... parameters) {
        Preconditions.checkArgument(parameters.length <= 1);
        if (parameters.length == 0) {
            return new WriteToken();
        } else {
            Preconditions.checkArgument(parameters[0] instanceof String);
            return new WriteToken((String) parameters[0]);
        }
    }

    public static class ReadToken implements SecurityToken.ReadToken {

        private final Authorizations authorizations;
        private final VisibilityEvaluator evaluator;

        public ReadToken() {
            this("");
        }

        public ReadToken(String authorizations) {
            Preconditions.checkArgument(authorizations != null);
            if (authorizations.isEmpty()) {
                this.authorizations = new Authorizations();
            } else {
                this.authorizations = new Authorizations(authorizations.split(","));
            }
            this.evaluator = new VisibilityEvaluator(this.authorizations);
        }

        public Authorizations getAuthorizations() {
            return authorizations;
        }

        @Override
        public boolean subsumes(SecurityToken.WriteToken token) {
            try {
                return evaluator.evaluate(((AccumuloSecurityToken.WriteToken) token).getVisibility());
            } catch (VisibilityParseException ex) {
                throw new IllegalArgumentException();
            }
        }

        @Override
        public String toString() {
            return authorizations.toString();
        }
    }

    public static class WriteToken implements SecurityToken.WriteToken {

        private final ColumnVisibility visibility;
        private final boolean isEmpty;

        public WriteToken() {
            this("");
        }

        public WriteToken(String visibility) {
            Preconditions.checkArgument(visibility != null);
            this.visibility = new ColumnVisibility(visibility);
            isEmpty = visibility.trim().isEmpty();
        }

        @Override
        public boolean isNull() {
            return isEmpty;
        }

        public ColumnVisibility getVisibility() {
            return visibility;
        }

        @Override
        public String toString() {
            String vis = visibility.toString();
            return vis.substring(1, vis.length() - 1);
        }
    }
}
