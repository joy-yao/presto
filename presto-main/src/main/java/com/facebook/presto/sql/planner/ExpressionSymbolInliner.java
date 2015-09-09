/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.DeReferenceExpression;

import java.util.Map;

public class ExpressionSymbolInliner
        extends ExpressionRewriter<Void>
{
    private final Map<Symbol, ? extends Expression> mappings;

    public ExpressionSymbolInliner(Map<Symbol, ? extends Expression> mappings)
    {
        this.mappings = mappings;
    }

    @Override
    public Expression rewriteDeReferenceExpression(DeReferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return mappings.get(Symbol.fromDeReference(node));
    }
}
