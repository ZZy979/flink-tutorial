package com.example.streaming;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Broadcast stream example: Given a stream of objects of different colors and shapes,
 * find pairs of objects of the same color that follow a certain pattern, e.g. a rectangle followed by a triangle.
 * Assume that the set of interesting patterns evolves over time.
 *
 * @see <a href="https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/datastream/fault-tolerance/broadcast_state/">The Broadcast State Pattern</a>
 */
public class ShapePatternMatcher {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Item stream: contains items of different colors and shapes
        DataStream<Item> itemStream = env.fromElements(
                new Item(1, "red", "□"),
                new Item(2, "blue", "△"),
                new Item(3, "blue", "□"),
                new Item(4, "red", "△"),
                new Item(5, "blue", "△"),
                new Item(6, "blue", "○"),
                new Item(7, "red", "○"),
                new Item(8, "blue", "△"),
                new Item(9, "blue", "□"),
                new Item(10, "blue", "□"),
                new Item(11, "blue", "△"),
                new Item(12, "red", "○"),
                new Item(13, "red", "□"),
                new Item(14, "blue", "○"),
                new Item(15, "red", "△"),
                new Item(16, "blue", "○"),
                new Item(17, "blue", "□"),
                new Item(18, "red", "○"),
                new Item(19, "blue", "□"),
                new Item(20, "red", "□")
        );

        // Rule stream: contains patterns for matching items
        DataStream<Rule> ruleStream = env.fromElements(
                new Rule("rule1", "□", "△"),
                new Rule("rule2", "△", "○")
        );

        // Key the items by color
        KeyedStream<Item, Color> colorPartitionedStream = itemStream
                .keyBy(item -> item.color);

        // Broadcast the rules and create the broadcast state
        BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                .broadcast(PatternMatchProcessFunction.ruleStateDescriptor);

        // Connect the two streams and specify match detecting logic
        DataStream<String> output = colorPartitionedStream
                .connect(ruleBroadcastStream)
                .process(new PatternMatchProcessFunction());

        output.print();
        env.execute();
    }


    // enum cannot be used as key
    public static class Color {
        public String name;

        public Color(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Color)) return false;
            Color color = (Color) o;
            return Objects.equals(name, color.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }
    }

    public static class Shape {
        public String name;

        public Shape(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Shape)) return false;
            Shape shape = (Shape) o;
            return Objects.equals(name, shape.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }
    }

    public static class Item {
        public int id;
        public Color color;
        public Shape shape;

        public Item(int id, String color, String shape) {
            this.id = id;
            this.color = new Color(color);
            this.shape = new Shape(shape);
        }

        @Override
        public String toString() {
            return "Item{id=" + id + ", color=" + color + ", shape=" + shape + '}';
        }
    }

    public static class Rule {
        public String name;
        public Shape first;
        public Shape second;

        public Rule(String name, String first, String second) {
            this.name = name;
            this.first = new Shape(first);
            this.second = new Shape(second);
        }

        @Override
        public String toString() {
            return "Rule{name=" + name + ", first=" + first + ", second=" + second + '}';
        }
    }

    public static class PatternMatchProcessFunction extends KeyedBroadcastProcessFunction<Color, Item, Rule, String> {
        // Store partial matches, i.e. first elements of the pair waiting for their second element
        // we keep a list as we may have many first elements waiting
        public static final MapStateDescriptor<String, List<Item>> mapStateDescriptor = new MapStateDescriptor<>(
                "items",
                BasicTypeInfo.STRING_TYPE_INFO,
                new ListTypeInfo<>(Item.class));

        // A map descriptor to store the name of the rule (string) and the rule itself.
        public static final MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
                "RulesBroadcastState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<Rule>() {}));

        @Override
        public void processBroadcastElement(Rule value, Context ctx, Collector<String> out) throws Exception {
            ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value);
        }

        @Override
        public void processElement(Item value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
            MapState<String, List<Item>> state = getRuntimeContext().getMapState(mapStateDescriptor);
            Shape shape = value.shape;

            for (Map.Entry<String, Rule> entry : ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
                String ruleName = entry.getKey();
                Rule rule = entry.getValue();

                List<Item> stored = state.get(ruleName);
                if (stored == null) {
                    stored = new ArrayList<>();
                }

                if (shape.equals(rule.second) && !stored.isEmpty()) {
                    for (Item i : stored) {
                        out.collect("MATCH " + ruleName + ": " + i + " - " + value);
                    }
                    stored.clear();
                }

                if (shape.equals(rule.first)) {
                    stored.add(value);
                }

                if (stored.isEmpty()) {
                    state.remove(ruleName);
                } else {
                    state.put(ruleName, stored);
                }
            }
        }
    }
}
