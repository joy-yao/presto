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
package com.facebook.presto.sql.tree;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class DeReferenceExpression
        extends Expression
{
    private final Optional<Expression> base; // base can be DeReference or FunctionCall or SubscriptExpression
    private final String fieldName;

    private QualifiedName longestQualifiedName;
    private boolean isQualifiedName;

    public DeReferenceExpression(String fieldName)
    {
        this(Optional.empty(), fieldName);
    }

    public DeReferenceExpression(Optional<Expression> base, String fieldName)
    {
        this.base = base;
        checkArgument(fieldName != null, "fieldName is null");
        this.fieldName = fieldName.toLowerCase();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDeReferenceExpression(this, context);
    }

    public Optional<Expression> getBase()
    {
        return base;
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public String getName()
    {
        if (!base.isPresent()) {
            return fieldName;
        }

        return base.get().toString() + "." + fieldName;
    }

    public boolean isQualifiedName()
    {
        if (longestQualifiedName == null) {
            calculateLongestQualifiedName();
        }
        return isQualifiedName;
    }

    public QualifiedName getLongestQualifiedName()
    {
        if (longestQualifiedName == null) {
            calculateLongestQualifiedName();
        }
        return longestQualifiedName;
    }

    private void calculateLongestQualifiedName()
    {
        if (!base.isPresent()) {
            longestQualifiedName = new QualifiedName(fieldName);
            isQualifiedName = true;
            return;
        }

        Expression baseExpression = base.get();
        if (baseExpression instanceof DeReferenceExpression) {
            DeReferenceExpression base = (DeReferenceExpression) baseExpression;

            if (base.isQualifiedName()) {
                isQualifiedName = true;
                longestQualifiedName = QualifiedName.of(base.getLongestQualifiedName(), fieldName);
            }
            else {
                longestQualifiedName = base.getLongestQualifiedName();
            }
            return;
        }
        else if (baseExpression instanceof SubscriptExpression) {
            SubscriptExpression base = (SubscriptExpression) baseExpression;
            if (base.getBase() instanceof DeReferenceExpression) {
                DeReferenceExpression deReferenceBase = (DeReferenceExpression) base.getBase();
            }
        }

        // FIXME: finish this.
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DeReferenceExpression that = (DeReferenceExpression) o;
        return Objects.equals(base, that.base) &&
                Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base, fieldName);
    }
}
