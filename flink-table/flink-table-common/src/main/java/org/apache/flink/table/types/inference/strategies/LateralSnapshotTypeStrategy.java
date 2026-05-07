/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Type strategies for the {@code SNAPSHOT} table function used by the {@code LATERAL SNAPSHOT}
 * temporal join.
 *
 * <p>Validates the named arguments
 *
 * <ul>
 *   <li>{@code input} (TABLE, required)
 *   <li>{@code load_completed_condition} (STRING literal, optional, default {@code
 *       'on_compile_time'}, allowed values: {@code 'on_compile_time'}, {@code 'on_time'})
 *   <li>{@code load_completed_time} (TIMESTAMP literal, optional)
 *   <li>{@code load_completed_idle_timeout} (INTERVAL DAY TO SECOND literal, optional)
 *   <li>{@code state_ttl} (INTERVAL DAY TO SECOND literal, optional)
 * </ul>
 *
 * <p>and ensures cross-argument consistency:
 *
 * <ul>
 *   <li>{@code load_completed_condition='on_time'} requires {@code load_completed_time}.
 *   <li>{@code load_completed_condition='on_compile_time'} (or unset) forbids {@code
 *       load_completed_time}.
 * </ul>
 *
 * <p>The output type passes the input table's row type through unchanged.
 */
@Internal
public final class LateralSnapshotTypeStrategy {

    /** Argument index of the {@code input} TABLE. */
    public static final int INPUT_ARG_INDEX = 0;

    /** Argument index of the {@code load_completed_condition} STRING. */
    public static final int LOAD_COMPLETED_CONDITION_ARG_INDEX = 1;

    /** Argument index of the {@code load_completed_time} TIMESTAMP. */
    public static final int LOAD_COMPLETED_TIME_ARG_INDEX = 2;

    /** Argument index of the {@code load_completed_idle_timeout} INTERVAL. */
    public static final int LOAD_COMPLETED_IDLE_TIMEOUT_ARG_INDEX = 3;

    /** Argument index of the {@code state_ttl} INTERVAL. */
    public static final int STATE_TTL_ARG_INDEX = 4;

    /** Default value for {@code load_completed_condition}. */
    public static final String LOAD_COMPLETED_CONDITION_ON_COMPILE_TIME = "on_compile_time";

    /**
     * Allowed value for {@code load_completed_condition} that requires {@code load_completed_time}.
     */
    public static final String LOAD_COMPLETED_CONDITION_ON_TIME = "on_time";

    private static final Set<String> VALID_LOAD_COMPLETED_CONDITIONS =
            Set.of(LOAD_COMPLETED_CONDITION_ON_COMPILE_TIME, LOAD_COMPLETED_CONDITION_ON_TIME);

    // --------------------------------------------------------------------------------------------
    // Input validation
    // --------------------------------------------------------------------------------------------

    public static final InputTypeStrategy INPUT_TYPE_STRATEGY =
            new InputTypeStrategy() {
                @Override
                public ArgumentCount getArgumentCount() {
                    return ConstantArgumentCount.between(1, 5);
                }

                @Override
                public Optional<List<DataType>> inferInputTypes(
                        final CallContext callContext, final boolean throwOnFailure) {
                    return validateInputs(callContext, throwOnFailure);
                }

                @Override
                public List<Signature> getExpectedSignatures(final FunctionDefinition definition) {
                    return List.of(
                            Signature.of(
                                    Argument.of("input", "TABLE"),
                                    Argument.of("load_completed_condition", "STRING"),
                                    Argument.of("load_completed_time", "TIMESTAMP"),
                                    Argument.of(
                                            "load_completed_idle_timeout",
                                            "INTERVAL DAY TO SECOND"),
                                    Argument.of("state_ttl", "INTERVAL DAY TO SECOND")));
                }
            };

    // --------------------------------------------------------------------------------------------
    // Output type inference: pass-through of input table row type.
    // --------------------------------------------------------------------------------------------

    public static final TypeStrategy OUTPUT_TYPE_STRATEGY =
            callContext -> {
                final TableSemantics semantics =
                        callContext
                                .getTableSemantics(INPUT_ARG_INDEX)
                                .orElseThrow(
                                        () ->
                                                new ValidationException(
                                                        "Argument 'input' of SNAPSHOT must be a table."));
                return Optional.of(semantics.dataType());
            };

    // --------------------------------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------------------------------

    private static Optional<List<DataType>> validateInputs(
            final CallContext callContext, final boolean throwOnFailure) {
        if (callContext.getTableSemantics(INPUT_ARG_INDEX).isEmpty()) {
            return callContext.fail(
                    throwOnFailure, "Argument 'input' of SNAPSHOT must be a table.");
        }

        // Reject non-literal load_completed_condition explicitly: the planner needs the value
        // at compile time to decide between 'on_compile_time' and 'on_time'.
        final boolean hasLoadCompletedCondition =
                isArgumentProvided(callContext, LOAD_COMPLETED_CONDITION_ARG_INDEX);
        if (hasLoadCompletedCondition
                && !callContext.isArgumentLiteral(LOAD_COMPLETED_CONDITION_ARG_INDEX)) {
            return callContext.fail(
                    throwOnFailure,
                    "Argument 'load_completed_condition' of SNAPSHOT must be a STRING literal.");
        }
        // Get condition and default to 'on_compile_time' if not provided
        final Optional<String> providedCondition =
                hasLoadCompletedCondition
                        ? callContext.getArgumentValue(
                                LOAD_COMPLETED_CONDITION_ARG_INDEX, String.class)
                        : Optional.empty();
        final String condition = providedCondition.orElse(LOAD_COMPLETED_CONDITION_ON_COMPILE_TIME);
        // Reject invalid condition value
        if (!VALID_LOAD_COMPLETED_CONDITIONS.contains(condition)) {
            return callContext.fail(
                    throwOnFailure,
                    String.format(
                            "Argument 'load_completed_condition' of SNAPSHOT must be one of %s "
                                    + "but was '%s'.",
                            VALID_LOAD_COMPLETED_CONDITIONS, condition));
        }

        // Check if load_completed_time is provided and a literal of the correct type if so.
        final boolean hasLoadCompletedTime =
                isArgumentProvided(callContext, LOAD_COMPLETED_TIME_ARG_INDEX);
        if (hasLoadCompletedTime
                && (!callContext.isArgumentLiteral(LOAD_COMPLETED_TIME_ARG_INDEX)
                        || !callContext
                                .getArgumentDataTypes()
                                .get(LOAD_COMPLETED_TIME_ARG_INDEX)
                                .getLogicalType()
                                .is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE))) {
            return callContext.fail(
                    throwOnFailure,
                    "Argument 'load_completed_time' of SNAPSHOT must be a TIMESTAMP literal.");
        }

        // Cross-argument consistency: condition <-> load_completed_time
        if (LOAD_COMPLETED_CONDITION_ON_TIME.equals(condition) && !hasLoadCompletedTime) {
            return callContext.fail(
                    throwOnFailure,
                    "SNAPSHOT requires 'load_completed_time' when "
                            + "'load_completed_condition' is 'on_time'.");
        }
        if (!LOAD_COMPLETED_CONDITION_ON_TIME.equals(condition) && hasLoadCompletedTime) {
            return callContext.fail(
                    throwOnFailure,
                    "SNAPSHOT does not accept 'load_completed_time' when "
                            + "'load_completed_condition' is not 'on_time'.");
        }

        // check that load_completed_idle_timeout is literal
        if (isArgumentProvided(callContext, LOAD_COMPLETED_IDLE_TIMEOUT_ARG_INDEX)
                && !callContext.isArgumentLiteral(LOAD_COMPLETED_IDLE_TIMEOUT_ARG_INDEX)) {
            return callContext.fail(
                    throwOnFailure,
                    "Argument 'load_completed_idle_timeout' of SNAPSHOT must be an INTERVAL literal.");
        }

        // check that state_ttl is literal
        if (isArgumentProvided(callContext, STATE_TTL_ARG_INDEX)
                && !callContext.isArgumentLiteral(STATE_TTL_ARG_INDEX)) {
            return callContext.fail(
                    throwOnFailure,
                    "Argument 'state_ttl' of SNAPSHOT must be an INTERVAL literal.");
        }

        return Optional.of(callContext.getArgumentDataTypes());
    }

    private static boolean isArgumentProvided(final CallContext callContext, final int index) {
        return callContext.getArgumentDataTypes().size() > index
                && !callContext.isArgumentNull(index);
    }

    private LateralSnapshotTypeStrategy() {}
}
