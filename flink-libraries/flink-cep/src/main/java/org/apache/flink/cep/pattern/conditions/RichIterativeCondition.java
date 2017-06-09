package org.apache.flink.cep.pattern.conditions;

import org.apache.flink.api.common.functions.AbstractRichFunction;

public abstract class RichIterativeCondition<T> extends AbstractRichFunction implements IterativeCondition<T> {
}
