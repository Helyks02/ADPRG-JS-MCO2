import fs from "fs";
import csv from "csv-parser";
import dayjs from "dayjs";
import { format } from "fast-csv";

const INPUT_FILE = "dpwh_flood_control_projects.csv";

function parseFloatSafe(v) {
        const n = parseFloat((v || "").toString().replace(/,/g, ""));
        return isNaN(n) ? 0 : n;
}

function parseDateSafe(v) {
        const d = dayjs(v);
        return d.isValid() ? d : null;
}

function median(arr) {
        if (!arr.length) return 0;

        const s = [...arr].sort((a, b) => a - b);
        const mid = Math.floor(s.length / 2);
        const median = s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;

        return median;
}

function average(arr) {
        const avg = arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;

        return avg;
}

async function loadCSV(filePath) {
        return new Promise((resolve, reject) => {
                const rows = [];
                fs.createReadStream(filePath)
                        .pipe(csv())
                        .on("data", (row) => rows.push(row))
                        .on("end", () => resolve(rows))
                        .on("error", (err) => reject(err));
        });
}

function cleanAndPrepareData(rawRows) {
        return rawRows
                .map((row) => {
                        const ApprovedBudgetForContract = parseFloatSafe(row.ApprovedBudgetForContract);
                        const ContractCost = parseFloatSafe(row.ContractCost);
                        const StartDate = parseDateSafe(row.StartDate);
                        const ActualCompletionDate = parseDateSafe(row.ActualCompletionDate);
                        const FundingYear = parseInt(row.FundingYear);
                        const Region = row.Region?.trim();
                        const MainIsland = row.MainIsland?.trim();
                        const Contractor = row.Contractor?.trim();
                        const TypeOfWork = row.TypeOfWork?.trim();

                        if (!FundingYear || FundingYear < 2021 || FundingYear > 2023) return null;
                        if (!StartDate || !ActualCompletionDate) return null;

                        const CostSavings = ApprovedBudgetForContract - ContractCost;
                        const CompletionDelayDays = ActualCompletionDate.diff(StartDate, "day");

                        return {
                                row,
                                ApprovedBudgetForContract,
                                ContractCost,
                                StartDate,
                                ActualCompletionDate,
                                FundingYear,
                                Region,
                                MainIsland,
                                Contractor,
                                TypeOfWork,
                                CostSavings,
                                CompletionDelayDays,
                        };
                })
                .filter(Boolean);
}

function generateReport1(data) {
        const regionMap = {};

        for (const r of data) {
                const key = `${r.Region}|${r.MainIsland}`;
                if (!regionMap[key])
                        regionMap[key] = {
                                Region: r.Region,
                                MainIsland: r.MainIsland,
                                budgets: [],
                                savings: [],
                                delays: [],
                        };
                regionMap[key].budgets.push(r.ApprovedBudgetForContract);
                regionMap[key].savings.push(r.CostSavings);
                regionMap[key].delays.push(r.CompletionDelayDays);
        }

        const results = Object.values(regionMap).map((grp) => {
                const avgDelay = average(grp.delays);
                const delayOver30 = (grp.delays.filter((d) => d > 30).length / grp.delays.length) * 100;
                let efficiency = (median(grp.savings) / avgDelay) * 100;
                if (!isFinite(efficiency) || efficiency < 0) efficiency = 0;
                efficiency = Math.min(100, Math.max(0, efficiency));

                return {
                        Region: grp.Region,
                        MainIsland: grp.MainIsland,
                        TotalApprovedBudget: grp.budgets.reduce((a, b) => a + b, 0).toFixed(2),
                        MedianCostSavings: median(grp.savings).toFixed(2),
                        AvgCompletionDelayDays: avgDelay.toFixed(2),
                        DelayOver30Percent: delayOver30.toFixed(2),
                        EfficiencyScore: efficiency.toFixed(2),
                };
        });

        results.sort((a, b) => b.EfficiencyScore - a.EfficiencyScore);
        return results;
}

function generateReport2(data) {
        const contractorMap = {};

        for (const r of data) {
                if (!r.Contractor) continue;
                const key = r.Contractor;
                if (!contractorMap[key])
                        contractorMap[key] = { Contractor: key, projects: 0, delays: [], totalSavings: 0, totalCost: 0 };
                contractorMap[key].projects++;
                contractorMap[key].delays.push(r.CompletionDelayDays);
                contractorMap[key].totalSavings += r.CostSavings;
                contractorMap[key].totalCost += r.ContractCost;
        }

        const results = Object.values(contractorMap)
                .filter((c) => c.projects >= 5)
                .map((c) => {
                        const avgDelay = average(c.delays);
                        let reliability = (1 - avgDelay / 90) * (c.totalSavings / c.totalCost) * 100;
                        if (!isFinite(reliability)) reliability = 0;
                        reliability = Math.min(100, Math.max(0, reliability));
                        return {
                                Contractor: c.Contractor,
                                Projects: c.projects,
                                AvgDelay: avgDelay.toFixed(2),
                                TotalCostSavings: c.totalSavings.toFixed(2),
                                ReliabilityIndex: reliability.toFixed(2),
                                RiskFlag: reliability < 50 ? "High Risk" : "OK",
                        };
                })
                .sort((a, b) => b.TotalCostSavings - a.TotalCostSavings)
                .slice(0, 15);

        return results;
}

function generateReport3(data) {
        const typeMap = {};

        for (const r of data) {
                const key = `${r.FundingYear}|${r.TypeOfWork}`;
                if (!typeMap[key])
                        typeMap[key] = { FundingYear: r.FundingYear, TypeOfWork: r.TypeOfWork, savings: [] };
                typeMap[key].savings.push(r.CostSavings);
        }

        const avgSavingsByYear = {};
        const results = Object.values(typeMap).map((grp) => {
                const avgSavings = average(grp.savings);
                avgSavingsByYear[grp.FundingYear] = avgSavingsByYear[grp.FundingYear] || [];
                avgSavingsByYear[grp.FundingYear].push(avgSavings);
                const overrunRate = (grp.savings.filter((s) => s < 0).length / grp.savings.length) * 100;
                return {
                        FundingYear: grp.FundingYear,
                        TypeOfWork: grp.TypeOfWork,
                        TotalProjects: grp.savings.length,
                        AvgCostSavings: avgSavings.toFixed(2),
                        OverrunRate: overrunRate.toFixed(2),
                };
        });

        const baseline = average(avgSavingsByYear[2021] || [0]);
        results.forEach((r) => {
                const yearAvg = average(avgSavingsByYear[r.FundingYear]);
                const change = r.FundingYear === 2021 ? 0 : ((yearAvg - baseline) / Math.abs(baseline || 1)) * 100;
                r.YoYChangePercent = change.toFixed(2);
        });

        results.sort((a, b) => a.FundingYear - b.FundingYear || b.AvgCostSavings - a.AvgCostSavings);
        return results;
}

function generateSummary(data, contractors, regions) {
        return {
                totalProjects: data.length,
                totalContractors: Object.keys(contractors).length,
                totalRegions: Object.keys(regions).length,
                avgGlobalDelay: average(data.map((r) => r.CompletionDelayDays)).toFixed(2),
                totalSavings: data.reduce((a, b) => a + b.CostSavings, 0).toFixed(2),
        };
}

async function writeCSV(filename, rows) {
        const ws = fs.createWriteStream(filename);
        const csvStream = format({ headers: true });
        csvStream.pipe(ws);
        for (const row of rows) csvStream.write(row);
        csvStream.end();
        await new Promise((resolve) => ws.on("finish", resolve));
        console.log(`Saved ${filename}`);
}

async function main() {
        console.log("Loading data...");
        const rawData = await loadCSV(INPUT_FILE);
        console.log(`Loaded ${rawData.length} raw rows.`);

        const data = cleanAndPrepareData(rawData);
        console.log(`Filtered & cleaned: ${data.length} rows`);

        const report1 = generateReport1(data);
        await writeCSV("report1.csv", report1);

        const report2 = generateReport2(data);
        await writeCSV("report2.csv", report2);

        const report3 = generateReport3(data);
        await writeCSV("report3.csv", report3);

        const summary = generateSummary(
                data,
                Object.fromEntries(report2.map((r) => [r.Contractor, r])),
                Object.fromEntries(report1.map((r) => [r.Region, r]))
        );
        fs.writeFileSync("summary.json", JSON.stringify(summary, null, 2));
        console.log("summary.json generated successfully");
}

main().catch((err) => console.error("Error:", err));
