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

public class DeReference
        extends Expression
{
    private final Expression base; // base can be DeReference or QualifiedNameReference or subscript
    private final String fieldName;

    public DeReference(Expression base, String fieldName)
    {
        this.base = base;
        this.fieldName = fieldName;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDeReference(this, context);
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
        if (base instanceof DeReference) {
            return ((DeReference) base).getName() + "." + fieldName;
        }
        if (base instanceof QualifiedNameReference) {
            return ((QualifiedNameReference) base).getName().toString() + "." + fieldName;
        }
//        if (base instanceof SubscriptExpression)
//            return ((SubscriptExpression) base).toString()

        return base.toString() + "." + fieldName;
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
        DeReference that = (DeReference) o;
        return Objects.equals(base, that.base) &&
                Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(base, fieldName);
    }
}
