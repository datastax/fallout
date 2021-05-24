/*
 * Copyright 2021 DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.fallout.ops;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import static com.datastax.fallout.ops.NodeGroup.State.TransitionDirection;
import static com.datastax.fallout.ops.NodeGroup.State.TransitionDirection.DOWN;
import static com.datastax.fallout.ops.NodeGroup.State.TransitionDirection.NONE;
import static com.datastax.fallout.ops.NodeGroup.State.TransitionDirection.UP;

class DumpNodeGroupStatesToDot
{
    private final PrintStream out;

    private DumpNodeGroupStatesToDot(PrintStream out)
    {
        this.out = out;
    }

    private void transition(NodeGroup.State start, NodeGroup.State end, NodeGroup.State.TransitionDirection direction)
    {
        if (direction == UP)
        {
            out.format("%s -> %s;\n", start, end);
        }
        else
        {
            out.format("%s -> %s [dir=back];\n", end, start);
        }
    }

    private final Map<TransitionDirection, List<NodeGroup.State>> groups = new HashMap<>();

    {
        for (TransitionDirection direction : TransitionDirection.values())
        {
            groups.put(direction, new ArrayList<>());
        }
    }

    private void state(NodeGroup.State state, NodeGroup.State.TransitionDirection direction)
    {
        out.format("%s [shape=%s, group=%s, style=%s, peripheries=%s, color=%s];\n",
            state,
            state.isTransitioningState() ? "ellipse" : "rect",
            state.isRunLevelState() ? "runlevel" : direction.toString(),
            !state.isConfigManagementState() ? "dashed" : "solid",
            state.isConfigManagementState() ? 2 : 1,
            state.isStarted() ? "green" : "black");
        groups.get(direction).add(state);
    }

    private void dumpNodeStates()
    {
        out.println("strict digraph test {");

        out.println("anchor [style=invis];");

        out.println("subgraph cluster_key {");
        out.println("label=key;");
        out.println("runlevel [shape=rect, group=side];");
        out.println("isTransitioningState [shape=ellipse, group=side];");
        out.println("isProvisioningState [style=dashed, group=side];");
        out.println("isConfigManagementState [peripheries=2, group=side];");
        out.println("isStarted [color=green, group=side];");
        out.println(
            "runlevel -> isTransitioningState -> isProvisioningState -> isConfigManagementState -> isStarted [style=invis];");
        out.println("}");

        out.println("anchor -> runlevel [style=invis];");

        List<NodeGroup.State> unknown = Arrays.stream(NodeGroup.State.values())
            .filter(NodeGroup.State::isUnknownState).collect(Collectors.toList());

        out.format("isStarted -> %s [style=invis];\n", unknown.get(0));

        out.println("subgraph cluster_unknown {");
        out.println("label=\"unknown states\";\n");
        unknown.forEach(state -> out.format("%s [shape=rect, group=side];\n", state));
        out.format("%s [style=invis];\n", Joiner.on(" -> ").join(unknown));
        out.println("}");

        List<NodeGroup.State> runLevels = Arrays.stream(NodeGroup.State.values())
            .filter(NodeGroup.State::isRunLevelState).collect(Collectors.toList());

        out.format("anchor-> %s [style=invis];\n", runLevels.get(0));

        runLevels.forEach(runLevel -> {
            state(runLevel, NONE);
        });

        runLevels.forEach(runLevel -> {
            Preconditions.checkState(runLevel.runLevel != null);

            if (runLevel.runLevel.up != null)
            {
                state(runLevel.runLevel.up, UP);
                transition(runLevel, runLevel.runLevel.up, UP);
                transition(runLevel.runLevel.up, runLevel.runLevel.upTransition().get().next, UP);
            }

            if (runLevel.runLevel.down != null)
            {
                state(runLevel.runLevel.down, DOWN);
                transition(runLevel, runLevel.runLevel.down, DOWN);
                transition(runLevel.runLevel.down, runLevel.runLevel.downTransition().get().next, DOWN);
            }
        });

        groups.values().forEach(states -> out.format("%s [style=invis];\n", Joiner.on(" -> ").join(states)));

        out.println("}");
    }

    public static void main(String[] args) throws IOException
    {
        if (args.length == 1)
        {
            try (PrintStream outFile = new PrintStream(args[0]))
            {
                new DumpNodeGroupStatesToDot(outFile).dumpNodeStates();
            }
        }
        else
        {
            new DumpNodeGroupStatesToDot(System.out).dumpNodeStates();
        }
    }
}
