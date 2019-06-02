const Apify = require("apify");
const { Parser } = require("json2csv");
const fs = require("fs");

Apify.main(async () => {
  const DataSetName = "test";

  const fields = ["fullname", "address", "phone"];
  const header = false;
  const opts = { fields, header };

  // Open a named dataset
  const dataset = await Apify.openDataset(DataSetName);
  const parser = new Parser(opts);

  const converterFunction = async (dataset, parser, filename) => {
    await dataset.forEach(async (item, index) => {
      if (item.results.length) {
        const lineBlock = await parser.parse(item.results);

        await fs.appendFile("message.txt", lineBlock, err => {
          if (err) throw err;
          console.log('The "data to append" was appended to file!');
        });
      }
    });
  };

  await converterFunction(dataset, parser, DataSetName + ".csv");

  console.log("Data Conversion finished.");
});
