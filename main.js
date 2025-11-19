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

        //... is a spread operator to create a shallow copy of the array
        const s = [...arr].sort((a, b) => a - b);
        // Find the middle index
        const mid = Math.floor(s.length / 2);
        // Calculate median based on even or odd length
        const median = s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;

        // Return median rounded to 2 decimal places
        return Number(median.toFixed(2));
}

/** average: Calculate the average of an array of numbers
 * @param {number[]} arr 
 * @returns average value
 */
function average(arr) {
        // Return 0 for empty array
        if (!arr.length) return 0;

        // Sum valid numbers and count them
        let sum = 0;
        let count = 0;

        // Iterate through array
        for (const x of arr) {
                // Convert to number
                const num = Number(x);
                // If valid, add to sum and increment count
                if (!isNaN(num)) {
                        sum += num;
                        count++;
                }
        }
        // Return average or 0 if no valid numbers
        return count ? sum / count : 0;
}

/** formatNumber: Format a number with fixed decimal places
 * @param {number} n 
 * @param {number} decimals 
 * @returns formatted string
 */
function formatNumber(n, decimals = 2) {
        // Handle null, undefined, empty string, or NaN cases and returns null
        if (n === null || n === undefined || n === "" || isNaN(n)) return null;
        return Number(parseFloat(n).toFixed(decimals));
}

/** loadCSV: Load CSV data from a file path
 * @param {string} filePath 
 * @returns Promise resolving to array of rows
 */
async function loadCSV(filePath) {
        // Return a promise that resolves when the file is fully read
        return new Promise((resolve, reject) => {
                // Array to hold rows
                const rows = [];
                // Create read stream and parse CSV
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
/** cleanAndPrepareData: Clean, impute, and prepare raw CSV data */
function cleanAndPrepareData(rawRows) {
        // Array to hold cleaned data
        const cleaned = [];

        // Iterate through each raw row
        for (const row of rawRows) {

                // Parse and validate FundingYear
                const FundingYear = parseInt(row.FundingYear);
                if (!FundingYear || FundingYear < 2021 || FundingYear > 2023)
                        continue;

                // Parse and validate ApprovedBudgetForContract
                const ApprovedBudgetForContract = parseFloatSafe(row.ApprovedBudgetForContract);
                if (ApprovedBudgetForContract === null || ApprovedBudgetForContract <= 0) {
                        continue;
                }

                // Parse and validate ContractCost
                const ContractCost = parseFloatSafe(row.ContractCost);
                if (ContractCost === null || ContractCost <= 0) {
                        continue;
                }

                // Parse StartDate
                const StartDate = parseDateSafe(row.StartDate);

                // Parse ActualCompletionDate
                const ActualCompletionDate = parseDateSafe(row.ActualCompletionDate);

                // Impute missing ActualCompletionDate with StartDate
                if (ActualCompletionDate === null || ActualCompletionDate === "") {
                        ActualCompletionDate = StartDate;
                }

                // ?.trim() to remove leading/trailing whitespace, and if null/undefined,
                // default to "Unknown" or appropriate placeholder
                const Region = row.Region?.trim() || "Unknown";
                const MainIsland = row.MainIsland?.trim() || "Unknown";
                const Contractor = row.Contractor?.trim() || "Unknown Contractor";
                const TypeOfWork = row.TypeOfWork?.trim() || "Unspecified";

                // Calculate CostSavings and CompletionDelayDays
                const CostSavings = ApprovedBudgetForContract - ContractCost;
                const CompletionDelayDays = ActualCompletionDate.diff(StartDate, "day");

                // Push cleaned and computed data to array
                cleaned.push({
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
                });
        }

        // Return cleaned data array
        return cleaned;
}

/** generateReport1: Generate Regional Flood Mitigation Efficiency Summary
 * @param {Array} data 
 * @returns report array
 */
function generateReport1(data) {
        // Group data by Region and MainIsland
        const regionMap = {};

        // Aggregate metrics for each region
        for (const r of data) {

                // Create unique key for region and main island
                const key = `${r.Region}|${r.MainIsland}`;

                // Initialize region entry if not exists
                if (!regionMap[key])
                        regionMap[key] = {
                                Region: r.Region,
                                MainIsland: r.MainIsland,
                                budgets: [],
                                savings: [],
                                delays: [],
                        };

                // Aggregate metrics
                regionMap[key].budgets.push(r.ApprovedBudgetForContract);
                regionMap[key].savings.push(r.CostSavings);
                regionMap[key].delays.push(r.CompletionDelayDays);
        }

        // Compute final metrics for each region
        const results = Object.values(regionMap).map((grp) => {

                // Calculate average delay, delay over 30 days percentage, and efficiency score
                const avgDelay = average(grp.delays);
                const delayOver30 = (grp.delays.filter((d) => d > 30).length / grp.delays.length) * 100;
                let efficiency = median(grp.savings) / avgDelay;

                // Handle edge cases for efficiency score
                if (!isFinite(efficiency) || efficiency < 0) efficiency = 0;

                // Return formatted result object
                return {
                        Region: grp.Region,
                        MainIsland: grp.MainIsland,
                        TotalApprovedBudget: formatNumber(grp.budgets.reduce((a, b) => a + b, 0)),
                        MedianCostSavings: formatNumber(median(grp.savings)),
                        AvgCompletionDelayDays: formatNumber(avgDelay, 2),
                        DelayOver30Percent: formatNumber(delayOver30, 2),
                        EfficiencyScore: efficiency
                };
        });

        // Normalize EfficiencyScore to 0-100 scale
        const efficiencies = results.map(r => r.EfficiencyScore);
        const minEff = Math.min(...efficiencies);
        const maxEff = Math.max(...efficiencies);

        // Adjust EfficiencyScore for each region
        results.forEach(r => {
                if (isFinite(r.EfficiencyScore) && maxEff !== minEff) {
                        r.EfficiencyScore = ((r.EfficiencyScore - minEff) / (maxEff - minEff)) * 100;
                } else {
                        r.EfficiencyScore = 0;
                }
                r.EfficiencyScore = Number(r.EfficiencyScore.toFixed(2));
        });

        // Sort results by EfficiencyScore in descending order
        results.sort((a, b) => b.EfficiencyScore - a.EfficiencyScore);

        // Return final results
        return results;
}


/** generateReport2: Generate Top Contractors Performance Summary
 * @param {Array} data 
 * @returns report array
 */
function generateReport2(data) {
        // Map to hold aggregated data by contractor
        const contractorMap = {};

        // Group and aggregate by contractor
        for (const r of data) {

                // Skip if Contractor is missing
                if (!r.Contractor) continue;

                // Initialize contractor entry if not exists
                const key = r.Contractor;

                // Create entry if not exists
                if (!contractorMap[key])
                        contractorMap[key] = {
                                Contractor: key,
                                projects: 0,
                                delays: [],
                                totalSavings: 0,
                                totalCost: 0
                        };

                // Aggregate metrics
                contractorMap[key].projects++;
                contractorMap[key].delays.push(r.CompletionDelayDays);
                contractorMap[key].totalSavings += r.CostSavings;
                contractorMap[key].totalCost += r.ContractCost;
        }

        // Filter, compute metrics, and prepare for sorting
        const results = Object.values(contractorMap)

                // Only include contractors with at least 5 projects
                .filter((c) => c.projects >= 5)

                // Compute reliability index and format results
                .map((c) => {

                        // Calculate average delay
                        const avgDelay = average(c.delays);

                        // Calculate reliability index
                        let reliability = (1 - (avgDelay / 90)) * (c.totalSavings / c.totalCost) * 100;

                        // Handle edge cases for reliability index
                        if (!isFinite(reliability)) reliability = 0;

                        // Cap reliability at 100
                        reliability = Math.min(100, reliability);

                        // Return formatted result object
                        return {
                                Contractor: c.Contractor,
                                TotalCost: formatNumber(c.totalCost),
                                NumProjects: c.projects,
                                AvgDelay: formatNumber(avgDelay, 2),
                                TotalSavings: formatNumber(c.totalSavings),
                                ReliabilityIndex: formatNumber(reliability, 2),
                                RiskFlag: reliability < 50 ? "High Risk" : "Low Risk",
                                _rawTotalCost: c.totalCost
                        };
                })

                // Sort by TotalCost in descending order and take top 15
                .sort((a, b) => b._rawTotalCost - a._rawTotalCost)
                .slice(0, 15)

                // Clean up temporary fields
                .map(c => {
                        delete c._rawTotalCost;
                        return c;
                });

        // Return final results
        return results;
}

/** generateReport3: Generate Cost Savings Trends by Funding Year and Type of Work
 * @param {Array} data 
 * @returns report array
 */
function generateReport3(data) {

        const typeMap = {};

        // Aggregate savings for each group
        for (const r of data) {

                // Create unique key for FundingYear and TypeOfWork
                const key = `${r.FundingYear}|${r.TypeOfWork}`;

                // Initialize group entry if not exists
                if (!typeMap[key]) {
                        typeMap[key] = {
                                FundingYear: r.FundingYear,
                                TypeOfWork: r.TypeOfWork,
                                savings: []
                        };
                }

                // Aggregate CostSavings
                typeMap[key].savings.push(r.CostSavings);
        }

        const yearTotals = {};
        const yearCounts = {};

        // Compute final metrics for each group
        const results = Object.values(typeMap).map((grp) => {

                // Calculate total projects and average savings
                const totalProjects = grp.savings.length;

                // Calculate average savings
                const avgSavings = average(grp.savings);

                // Accumulate totals for weighted average calculation
                const year = Number(grp.FundingYear);
                yearTotals[year] = (yearTotals[year] || 0) + grp.savings.reduce((a, b) => a + b, 0);
                yearCounts[year] = (yearCounts[year] || 0) + grp.savings.length;

                // Calculate overrun rate
                const overrunRate = grp.savings.length ? (grp.savings.filter((s) => s < 0).length / grp.savings.length) * 100 : 0;

                // Return formatted result object
                return {
                        FundingYear: year,
                        TypeOfWork: grp.TypeOfWork,
                        TotalProjects: totalProjects,
                        AvgCostSavings: formatNumber(avgSavings),
                        _rawAvgSavings: avgSavings,
                        OverrunRate: formatNumber(overrunRate),
                };
        });

        // Calculate weighted average savings per year
        const weightedYearAvg = {};
        for (const yearStr in yearTotals) {
                const year = Number(yearStr);
                weightedYearAvg[year] = yearTotals[year] / yearCounts[year];
        }

        // Calculate Year-over-Year change compared to 2021 baseline
        const baseline = weightedYearAvg[2021] || 0;

        // Add YoYChange to each result
        results.forEach((r) => {
                const yearAvg = weightedYearAvg[r.FundingYear] || 0;
                const change = r.FundingYear === 2021 ? 0 : ((yearAvg - baseline) / Math.abs(baseline || 1)) * 100;
                r.YoYChange = change.toFixed(2);
        });

        // Sort results by FundingYear ascending, then AvgCostSavings descending
        results.sort((a, b) => {
                if (a.FundingYear !== b.FundingYear) return a.FundingYear - b.FundingYear;
                return b._rawAvgSavings - a._rawAvgSavings;
        });

        // Clean up temporary fields
        results.forEach(r => delete r._rawAvgSavings);

        // Return final results
        return results;
}



/** generateSummary: Generate summary statistics
 * @param {Array} data 
 * @param {Object} contractors
 * @param {Object} regions
 * @returns summary object
 */
function generateSummary(data, contractors, regions) {
        // Calculate average global delay
        return {
                totalProjects: data.length,
                totalContractors: Object.keys(contractors).length,
                totalRegions: Object.keys(regions).length,
                avgGlobalDelay: formatNumber(average(data.map((r) => r.CompletionDelayDays))),
                totalSavings: formatNumber(data.reduce((a, b) => a + b.CostSavings, 0)),
        };
}

/**
 * formatCell: Format cell value for display in preview
 * @param {*} value 
 * @returns value as formatted string
 */
function formatCell(value) {
        // Handle null or undefined
        if (value === null || value === undefined) return "";

        // If value is already a string, trim it to check for empty strings
        const strVal = value.toString().trim();
        if (strVal === "") return "";

        // Try to parse it as a number
        const num = Number(value);

        // Check if it is a valid number (and not NaN)
        if (!isNaN(num)) {
                // 'en-US' ensures commas are used for thousands (e.g., 1,000.00)
                return num.toLocaleString('en-US');
        }

        // Return original value if it's not a number
        return value.toString();
}

/** previewFile: Preview the first few lines of a CSV file in a formatted table
 * @param {string} filePath 
 * @param {number} lines
 * @param {string} reportTitle
 * @param {string} filterNote
 * @param {number} reportNumber
 * @returns void
 */
async function previewFile(filePath, lines, reportTitle = "", filterNote = "", reportNumber) {

        // Read and preview the first few lines of the CSV file
        try {

                // Check if file exists
                if (!fs.existsSync(filePath)) {
                        console.log(`File not found: ${filePath}`);
                        return;
                }

                // Print report header
                console.log(`\nReport ${reportNumber}: ${reportTitle}`);

                // Print filter note if provided
                console.log(`${reportTitle}`);
                if (filterNote) console.log(`(${filterNote})`);
                console.log("");

                // Read the CSV file and collect the first few lines
                const results = [];

                // Read CSV and collect lines
                await new Promise((resolve, reject) => {
                        fs.createReadStream(filePath)
                                .pipe(csv())
                                .on("data", (data) => {
                                        if (results.length < lines) results.push(data);
                                })
                                .on("end", resolve)
                                .on("error", reject);
                });

                // If no data, print message
                if (results.length === 0) {
                        console.log(`No data to preview in ${filePath}`);
                        return;
                }

                // Determine column widths for formatting
                const headers = Object.keys(results[0]);

                const colWidths = headers.map(h => {
                        const maxContentLength = Math.max(
                                ...results.map(r => {
                                        const val = r[h];
                                        return val ? formatCell(val).length : 0;
                                })
                        );
                        return Math.max(h.length, maxContentLength) + 2;
                });

                // Print header row
                const headerRow = headers.map((h, i) => h.padEnd(colWidths[i])).join(" | ");
                console.log(headerRow);
                console.log("-".repeat(headerRow.length));

                // Print data rows
                for (const r of results) {
                        const rowStr = headers.map((h, i) => {
                                // Apply formatting before padding
                                const formattedValue = formatCell(r[h]);
                                return formattedValue.padEnd(colWidths[i]);
                        }).join(" | ");

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

        // Write rows to CSV file
        const ws = fs.createWriteStream(filename);

        // Use fast-csv to write CSV with headers
        const csvStream = format({ headers: true });
        csvStream.pipe(ws);

        // Write each row
        for (const row of rows) csvStream.write(row);
        csvStream.end();

        // Wait for the stream to finish
        await new Promise((resolve) => ws.on("finish", resolve));
}

/** generateReports: Generate all reports and save to CSV files
 * @returns void
 */
async function generateReports() {

        // Ensure data is loaded
        if (!loadedData) {
                console.log("Error: No data loaded. Please load the CSV file first (option 1).");
                return;
        }

        // Generate each report
        console.log("");
        console.log("Generating reports...");

        // Clean and prepare data
        const data = cleanAndPrepareData(loadedData);
        if (!data.length) {
                console.log("No valid data after cleaning. Please check your CSV file.");
                return;
        }

        // Report 1: Regional Flood Mitigation Efficiency Summary
        const report1 = generateReport1(data);
        const file1 = "report1_regional_summary.csv";
        await writeCSV(file1, report1);
        await previewFile(
                file1,
                2,
                "Regional Flood Mitigation Efficiency Summary",
                "Filtered: Projects from 2021–2023 only",
                1
        );
        console.log(`(Full table exported to ${file1})`);

        // Report 2: Top Contractors Performance Summary
        const report2 = generateReport2(data);
        const file2 = "report2_contractor_ranking.csv";
        await writeCSV(file2, report2);
        await previewFile(
                file2,
                2,
                "Top Contractors Performance Summary",
                "Top 15 by TotalCost, >= 5 Projects",
                2
        );
        console.log(`(Full table exported to ${file2})`);

        // Report 3: Cost Savings Trends by Funding Year and Type of Work
        const report3 = generateReport3(data);
        const file3 = "report3_cost_trends.csv";
        await writeCSV(file3, report3);
        await previewFile(
                file3,
                3,
                "Cost Savings Trends by Funding Year and Type of Work",
                "Grouped by FundingYear & TypeOfWork",
                3
        );
        console.log(`(Full table exported to ${file3})`);

        // Summary Stats
        const summary = generateSummary(
                data,
                Object.fromEntries(report2.map((r) => [r.Contractor, r])),
                Object.fromEntries(report1.map((r) => [r.Region, r]))
        );

        // Write summary to JSON file
        const summaryFile = "summary.json";
        fs.writeFileSync(summaryFile, JSON.stringify(summary, null, 2));
        console.log(`Summary Stats (${summaryFile})`);

        // Read and display summary preview
        const summaryData = JSON.parse(fs.readFileSync(summaryFile, "utf8"));

        // Display summary preview
        console.log("\nSummary Preview:");
        console.log(JSON.stringify({
                global_avg_delay: parseFloat(summaryData.avgGlobalDelay)
                        .toLocaleString('en-US',
                                {
                                        minimumFractionDigits: 2,
                                        maximumFractionDigits: 2
                                }),
                total_savings: parseFloat(summaryData.totalSavings)
                        .toLocaleString('en-US',
                                {
                                        minimumFractionDigits: 2,
                                        maximumFractionDigits: 2
                                }),
        }, null, 0));

}

/** reportSelection: Prompt user to return to report selection or exit
 * @returns void
 */
function reportSelection() {
        // Prompt user to return to main menu or exit
        let choice;

        // Loop until valid input
        while (true) {

                choice = scan.question("Back to Report Selection (Y/N): ");
                choice = choice.toUpperCase();

                // Handle user choice
                if (choice === 'Y') {
                        main();
                        break;
                } else if (choice === 'N') {
                        console.log("Exiting the program.");
                        process.exit(0);
                } else {
                        console.log("Invalid choice. Please enter Y or N.");
                }
        }
}

/** main: Main program loop for user interaction
 * @returns void
 */
async function main() {
        // Display menu and handle user choices
        console.log("Select Language Implementation:");
        console.log("[1] Load the File");
        console.log("[2] Generate Reports");
        console.log("");

        let choice;

        // Loop until valid input
        while (true) {
                choice = parseInt(scan.question("Enter choice: "));

                // Handle user choice
                // 1: Load file, 2: Generate reports
                if (choice === 1) {
                        try {
                                // Load and clean data
                                const rows = await loadCSV(INPUT_FILE);
                                const cleaned = cleanAndPrepareData(rows);
                                console.log(`Processing dataset... (${rows.length} rows loaded, ${cleaned.length} filtered for 2021-2023)`);
                                console.log("");

                                // Update global loadedData with cleaned data
                                loadedData = cleaned;

                                // Restart main menu
                                await main();

                                // Break the loop to avoid duplicate prompts
                                break;

                        } catch (err) {
                                console.error("Error loading file:", err);
                        }
                } else if (choice === 2) {

                        // Generate reports
                        await generateReports();

                        // Prompt for next action
                        reportSelection();

                        // Break the loop to avoid duplicate prompts
                        break;
                } else {
                        console.log("Invalid choice. Please enter 1 or 2.");
                }
        }
}

// Start the program
main();
