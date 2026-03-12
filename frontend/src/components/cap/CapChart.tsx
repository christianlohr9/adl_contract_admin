import {
  ChartContainer,
  ChartTooltip,
  ChartTooltipContent,
  type ChartConfig,
} from "@/components/ui/chart";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid } from "recharts";
import { formatContractType } from "@/lib/format";

const chartConfig = {
  NG: {
    label: "Negotiated",
    color: "hsl(221, 83%, 53%)",
  },
  SD: {
    label: "Supplemental Draft",
    color: "hsl(142, 71%, 45%)",
  },
  FG: {
    label: "Free Agent",
    color: "hsl(38, 92%, 50%)",
  },
} satisfies ChartConfig;

interface CapChartProps {
  salaryByType: Record<string, number>;
}

export function CapChart({ salaryByType }: CapChartProps) {
  const data = [
    {
      name: "Salary",
      NG: salaryByType["NG"] ?? 0,
      SD: salaryByType["SD"] ?? 0,
      FG: salaryByType["FG"] ?? 0,
    },
  ];

  return (
    <ChartContainer config={chartConfig} className="h-[120px] w-full [&>div]:!aspect-auto">
      <BarChart data={data} layout="vertical">
        <CartesianGrid horizontal={false} />
        <XAxis type="number" tickFormatter={(v) => `$${v}`} />
        <YAxis type="category" dataKey="name" hide />
        <ChartTooltip
          content={
            <ChartTooltipContent
              formatter={(value, name) =>
                `${formatContractType(String(name))}: $${Number(value).toFixed(2)}`
              }
            />
          }
        />
        <Bar
          dataKey="NG"
          stackId="salary"
          fill="var(--color-NG)"
          radius={[0, 0, 0, 0]}
        />
        <Bar
          dataKey="SD"
          stackId="salary"
          fill="var(--color-SD)"
          radius={[0, 0, 0, 0]}
        />
        <Bar
          dataKey="FG"
          stackId="salary"
          fill="var(--color-FG)"
          radius={[4, 4, 4, 4]}
        />
      </BarChart>
    </ChartContainer>
  );
}
