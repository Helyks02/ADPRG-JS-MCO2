// ********************
// Last names: De Leon, Lee, Ortega, Robenta
// Language: JavaScript
// Paradigm(s): Procedural, Functional
// ********************

/**
 * fs module: File system operations
 * csv-parser: CSV parsing
 * dayjs: Date parsing and manipulation
 * fast-csv: CSV writing
 * readline-sync: Synchronous user input
 */
import fs from "fs";
import csv from "csv-parser";
import dayjs from "dayjs";
import { format } from "fast-csv";
import scan from "readline-sync";

/**
 * Global Constants and Variables
 * INPUT_FILE: Path to the input CSV file
 * loadedData: Variable to hold loaded CSV data
 */
const INPUT_FILE = "dpwh_flood_control_projects.csv";
let loadedData = null;

/**
 * parseFloatSafe: Safely parse a float from a string, handling commas and invalid numbers
 * @param {*} v 
 * @returns 0 if n is not a number, and n if n is a number
 */
function parseFloatSafe(v) {
        const n = parseFloat((v || "").toString().replace(/,/g, ""));
        return isNaN(n) ? 0 : n;
}

/** parseDateSafe: Safely parse a date using dayjs, returning null for invalid dates
 * @param {*} v 
 * @returns dayjs object or null
 */
function parseDateSafe(v) {
        const d = dayjs(v);
        return d.isValid() ? d : null;
}

/** median: Calculate the median of an array of numbers
 * @param {number[]} arr 
 * @returns median value
 */
function median(arr) {
        if (!arr.length) return 0;

        const s = [...arr].sort((a, b) => a - b);
        const mid = Math.floor(s.length / 2);
        const median = s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;

        return Number(median.toFixed(2));
}

/** average: Calculate the average of an array of numbers
 * @param {number[]} arr 
 * @returns average value
 */
function average(arr) {
        const avg = arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;

        return avg;
}

/** formatNumber: Format a number with commas and fixed decimal places
 * @param {number} n 
 * @param {number} decimals 
 * @returns formatted string
 */
function formatNumber(n, decimals = 2) {
        if (isNaN(n)) return "0.00";
        return n.toLocaleString("en-US", { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

/** loadCSV: Load CSV data from a file path
 * @param {string} filePath 
 * @returns Promise resolving to array of rows
 */
async function loadCSV(filePath) {
        return new Promise((resolve, reject) => {
                const rows = [];
                fs.createReadStream(filePath)
                        .pipe(csv())
                        .on("data", (row) => rows.push(row))
                        .on("end", () => {
                                loadedData = rows;
                                resolve(rows);
                        })
                        .on("error", (err) => reject(err));
        });
}

/** cleanAndPrepareData: Clean and prepare raw CSV data for analysis
 * @param {Array} rawRows 
 * @returns cleaned data array
 */
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

/** generateReport1: Generate Regional Flood Mitigation Efficiency Summary
 * @param {Array} data 
 * @returns report array
 */
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
                        TotalApprovedBudget: formatNumber(grp.budgets.reduce((a, b) => a + b, 0)),
                        MedianCostSavings: formatNumber(median(grp.savings)),
                        AvgCompletionDelayDays: formatNumber(avgDelay, 2),
                        DelayOver30Percent: formatNumber(delayOver30, 2),
                        EfficiencyScore: formatNumber(efficiency, 2),
                };

        });

        results.sort((a, b) => b.EfficiencyScore - a.EfficiencyScore);
        return results;
}

/** generateReport2: Generate Top Contractors Performance Summary
 * @param {Array} data 
 * @returns report array
 */
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
                        let reliability = (1 - (avgDelay / 90)) * (c.totalSavings / c.totalCost) * 100;
                        if (!isFinite(reliability)) reliability = 0;
                        reliability = Math.min(100, Math.max(0, reliability));
                        return {
                                Contractor: c.Contractor,
                                Projects: c.projects,
                                AvgDelay: formatNumber(avgDelay),
                                TotalCostSavings: formatNumber(c.totalSavings),
                                ReliabilityIndex: formatNumber(reliability),
                                RiskFlag: reliability < 50 ? "High Risk" : "OK",
                        };

                })
                .sort((a, b) => b.TotalCostSavings - a.TotalCostSavings)
                .slice(0, 15);

        return results;
}

/** generateReport3: Generate Cost Savings Trends by Funding Year and Type of Work
 * @param {Array} data 
 * @returns report array
 */
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
                        AvgCostSavings: formatNumber(avgSavings),
                        OverrunRate: formatNumber(overrunRate),
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

/** generateSummary: Generate summary statistics
 * @param {Array} data 
 * @param {Object} contractors
 * @param {Object} regions
 * @returns summary object
 */
function generateSummary(data, contractors, regions) {
        return {
                totalProjects: data.length,
                totalContractors: Object.keys(contractors).length,
                totalRegions: Object.keys(regions).length,
                avgGlobalDelay: formatNumber(average(data.map((r) => r.CompletionDelayDays))),
                totalSavings: formatNumber(data.reduce((a, b) => a + b.CostSavings, 0)),
        };
}

/** previewFile: Preview the first few lines of a CSV file in a formatted table
 * @param {string} filePath 
 * @param {number} lines
 * @param {string} reportTitle
 * @param {string} filterNote
 * @param {number} reportNumber
 * @returns void
 */
async function previewFile(filePath, lines = 5, reportTitle = "", filterNote = "", reportNumber) {
    try {
        if (!fs.existsSync(filePath)) {
            console.log(`File not found: ${filePath}`);
            return;
        }

        console.log(`\nReport ${reportNumber}: ${reportTitle}`);
        console.log(`${reportTitle}`);
        if (filterNote) console.log(`(${filterNote})`);
        console.log("");

        const results = [];
        await new Promise((resolve, reject) => {
            fs.createReadStream(filePath)
                .pipe(csv())
                .on("data", (data) => {
                    if (results.length < lines) results.push(data);
                })
                .on("end", resolve)
                .on("error", reject);
        });

        if (results.length === 0) {
            console.log(`No data to preview in ${filePath}`);
            return;
        }

        const headers = Object.keys(results[0]);

        const colWidths = headers.map(h => Math.max(h.length, ...results.map(r => (r[h] ? r[h].toString().length : 0))) + 2);

        const headerRow = headers.map((h, i) => h.padEnd(colWidths[i])).join(" | ");
        console.log(headerRow);
        console.log("-".repeat(headerRow.length));

        for (const r of results) {
            const rowStr = headers.map((h, i) => (r[h] ? r[h].toString().padEnd(colWidths[i]) : " ".repeat(colWidths[i]))).join(" | ");
            console.log(rowStr);
        }

        console.log("");
    } catch (err) {
        console.error(`Error previewing ${filePath}:`, err);
    }
}


/** writeCSV: Write an array of objects to a CSV file
 * @param {string} filename 
 * @param {Array} rows
 * @returns Promise<void>
 */
async function writeCSV(filename, rows) {
        const ws = fs.createWriteStream(filename);
        const csvStream = format({ headers: true });
        csvStream.pipe(ws);
        for (const row of rows) csvStream.write(row);
        csvStream.end();
        await new Promise((resolve) => ws.on("finish", resolve));
}

/** generateReports: Generate all reports and save to CSV files
 * @returns void
 */
async function generateReports() {
        if (!loadedData) {
                console.log("Error: No data loaded. Please load the CSV file first (option 1).");
                return;
        }

        console.log("Generating reports...");

        const data = cleanAndPrepareData(loadedData);
        if (!data.length) {
                console.log("No valid data after cleaning. Please check your CSV file.");
                return;
        }

        const report1 = generateReport1(data);
        const file1 = "report1_regional_summary.csv";
        await writeCSV(file1, report1);
        await previewFile(
                file1,
                3,
                "Regional Flood Mitigation Efficiency Summary",
                "Filtered: Projects from 2021–2023 only",
                1
        );
        console.log(`(Full table exported to ${file1})`);

        const report2 = generateReport2(data);
        const file2 = "report2_contractor_ranking.csv";
        await writeCSV(file2, report2);
        await previewFile(
                file2,
                3,
                "Top Contractors Performance Summary",
                "Top 15 by TotalCost, >= 5 Projects",
                2
        );
        console.log(`(Full table exported to ${file2})`);

        const report3 = generateReport3(data);
        const file3 = "report3_cost_trends.csv";
        await writeCSV(file3, report3);
        await previewFile(
                file3,
                4,
                "Cost Savings Trends by Funding Year and Type of Work",
                "Grouped by FundingYear & TypeOfWork",
                3
        );
        console.log(`(Full table exported to ${file3})`);

        const summary = generateSummary(
                data,
                Object.fromEntries(report2.map((r) => [r.Contractor, r])),
                Object.fromEntries(report1.map((r) => [r.Region, r]))
        );

        const summaryFile = "summary.json";
        fs.writeFileSync(summaryFile, JSON.stringify(summary, null, 2));
        console.log(`Summary Stats (${summaryFile})`);

        const summaryData = JSON.parse(fs.readFileSync(summaryFile, "utf8"));
        console.log("\nSummary Preview:");
        console.table(summaryData);
}

/** reportSelection: Prompt user to return to report selection or exit
 * @returns void
 */
function reportSelection() {
        let choice = scan.question("Back to Report Selection (Y/N): ");
        choice = choice.toUpperCase();
        while (true) {
                if (choice === 'Y') {
                        main();
                        break;
                } else if (choice === 'N') {
                        console.log("Exiting the program.");
                        process.exit(0);
                } else {
                        console.log("Invalid choice.");
                        choice = scan.question("Back to Report Selection (Y/N): ");
                }
        }
}

/** main: Main program loop for user interaction
 * @returns void
 */
async function main() {
        console.log("Select Language Implementation:");
        console.log("1. Load the File");
        console.log("2. Generate Reports");

        let choice = parseInt(scan.question("Enter choice: "));

        if (choice === 1) {
                try {
                        const rows = await loadCSV(INPUT_FILE);
                        const cleaned = cleanAndPrepareData(rows);
                        console.log(`Processing dataset... (${rows.length} rows loaded, ${cleaned.length} filtered for 2021-2023)`);

                        loadedData = cleaned;

                        main();
                } catch (err) {
                        console.error("Error loading file:", err);
                }
        } else if (choice === 2) {
                await generateReports();
                reportSelection();
        } else {
                console.log("Invalid choice.");
        }
}

// Start the program
main();
