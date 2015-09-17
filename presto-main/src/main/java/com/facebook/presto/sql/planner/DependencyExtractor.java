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

import com.facebook.presto.sql.tree.DeReferenceExpression;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

public final class DependencyExtractor
{
    private DependencyExtractor() {}

    public static Set<Symbol> extractUnique(Expression expression)
    {
        return ImmutableSet.copyOf(extractAll(expression));
    }

    public static Set<Symbol> extractUnique(Iterable<? extends Expression> expressions)
    {
        ImmutableSet.Builder<Symbol> unique = ImmutableSet.builder();
        for (Expression expression : expressions) {
            unique.addAll(extractAll(expression));
        }
        return unique.build();
    }

    public static List<Symbol> extractAll(Expression expression)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        new SymbolBuilderVisitor().process(expression, builder);
        return builder.build();
    }

    // to extract qualified name with prefix
    public static Set<QualifiedName> extractNames(Expression expression)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        new QualifiedNameBuilderVisitor().process(expression, builder);
        return builder.build();
    }

    private static class SymbolBuilderVisitor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Symbol>>
    {
        @Override
        protected Void visitDeReferenceExpression(DeReferenceExpression node, ImmutableList.Builder<Symbol> builder)
        {
//            builder.add(Symbol.fromDeReference(node));
            if (!node.getBase().isPresent()) {
                builder.add(Symbol.fromDeReference(node));
            }
            return null;
        }
    }

    private static class QualifiedNameBuilderVisitor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<QualifiedName>>
    {
        @Override
        protected Void visitDeReferenceExpression(DeReferenceExpression node, ImmutableSet.Builder<QualifiedName> builder)
        {
//            checkState(node.isQualifiedName(), "DeReference node is not a qualified name");
            builder.add(node.getLongestQualifiedName());
            return null;
        }
    }
}
