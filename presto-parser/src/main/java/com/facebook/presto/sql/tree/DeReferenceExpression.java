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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class DeReferenceExpression
        extends Expression
{
    private final Expression base;
    private final String fieldName;

    private QualifiedName qualifiedName;
    private boolean isQualifiedName;
    private boolean qualifiedNameCalculated;

    public DeReferenceExpression(Expression base, String fieldName)
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

    public Expression getBase()
    {
        return base;
    }

    public String getFieldName()
    {
        return fieldName;
    }

    public String getName()
    {
        return base.toString() + "." + fieldName;
    }

    public boolean isQualifiedName()
    {
        if (!qualifiedNameCalculated) {
            calculateQualifiedName();
        }
        return isQualifiedName;
    }

    public QualifiedName getQualifiedName()
    {
        if (!qualifiedNameCalculated) {
            calculateQualifiedName();
        }
        checkState(isQualifiedName, "This DeReferenceExpression is not a qualifiedName");
        return qualifiedName;
    }

    private void calculateQualifiedName()
    {
        if (base instanceof DeReferenceExpression) {
            DeReferenceExpression baseExpression = (DeReferenceExpression) base;
            if (baseExpression.isQualifiedName()) {
                isQualifiedName = true;
                qualifiedName = QualifiedName.of(baseExpression.getQualifiedName(), fieldName);
            }
        }
        else if (base instanceof QualifiedNameReference) {
            isQualifiedName = true;
            qualifiedName = QualifiedName.of(((QualifiedNameReference) base).getName(), fieldName);
        }
        qualifiedNameCalculated = true;
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
