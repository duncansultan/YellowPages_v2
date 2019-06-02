const Apify = require('apify');

// Apify.utils contains various utilities, e.g. for logging.
// Here we turn off the logging of unimportant messages.
const { log } = Apify.utils;
log.setLevel(log.LEVELS.WARNING);

const GoogleSheetsKey = '1KAipR2jR9TKMKmsVhyZgHKnt4Uhk4AJaylbUjcW17MQ';
const CSV_LINK = `https://docs.google.com/spreadsheets/d/${GoogleSheetsKey}/gviz/tq?tqx=out:csv`;
const DataSetName = 'NC';

// Apify.main() function wraps the crawler logic (it is optional).
Apify.main(async () => {
    // Create an instance of the RequestList class that contains a list of URLs to crawl.
    // Here we download and parse the list of URLs from an external file.
    const requestList = new Apify.RequestList({
        sources: [{ requestsFromUrl: CSV_LINK }],
    }); 
    await requestList.initialize();

    const requestQueue = await Apify.openRequestQueue();

    // Open a named dataset
    const dataset = await Apify.openDataset(DataSetName);

    // Create an instance of the CheerioCrawler class - a crawler
    // that automatically loads the URLs and parses their HTML using the cheerio library.
    const crawler = new Apify.CheerioCrawler({
        // Let the crawler fetch URLs from our list.
        requestList,
        requestQueue,
        // The crawler downloads and processes the web pages in parallel, with a concurrency
        // automatically managed based on the available system memory and CPU (see AutoscaledPool class).
        // Here we define some hard limits for the concurrency.
        minConcurrency: 1,
        maxConcurrency: 1,

        // On error, retry each page at most once.
        maxRequestRetries: 1,

        // Increase the timeout for processing of each page.
        handlePageTimeoutSecs: 60,

        // This function will be called for each URL to crawl.
        // It accepts a single parameter, which is an object with the following fields:
        // - request: an instance of the Request class with information such as URL and HTTP method
        // - html: contains raw HTML of the page
        // - $: the cheerio object containing parsed HTML
        handlePageFunction: async ({ request, response, html, $ }) => {
            console.log(`Processing ${request.url}...`);

            // Extract data from the page using cheerio.
            let title = $('.phone-result-label').text();
                     
            const results = [];
            $('.phone-result').each((index, el) => {               
                let fullname =  $(el).find('.fullname').text();
                let address =  $(el).find('.address').text();
                let phone =  $(el).find('.phone').text();

                results.push({
                    fullname,
                    address,
                    phone
                });
            });
            
            // Store the results to the default dataset. In local configuration,
            // the data will be stored as JSON files in ./apify_storage/datasets/default
            await dataset.pushData({
                url: request.url,
                title,
                results,
                html,
            });

            await Apify.utils.enqueueLinks({
                $,
                requestQueue,
                selector: 'a:contains("Next")',
                baseUrl: 'https://people.yellowpages.com'
            });
        },

        // This function is called if the page processing failed more than maxRequestRetries+1 times.
        handleFailedRequestFunction: async ({ request }) => {
            console.log(`Request ${request.url} failed twice.`);
        },
    });

    // Run the crawler and wait for it to finish.
    await crawler.run();

    console.log('Crawler finished.');
});