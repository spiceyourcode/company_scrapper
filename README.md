# Company Data Scraper

## Installation

1. Clone the repository:
```
git clone https://github.com/your-username/company-data-scraper.git
```

2. Install the required dependencies:
```
pip install -r requirements.txt
```

## Usage

1. Prepare an input file (CSV or XLSX) with a 'Business Name' column containing the company names to be scraped.
2. Update the `INPUT_FILENAME` and `OUTPUT_FILENAME` variables in the `scraper.py` file.
3. Run the script:
```
python scraper.py
```
4. The processed data will be saved to the specified `OUTPUT_FILENAME`.

## API

The script provides the following functions:

- `process_company(company_name)`: Scrapes data for a single company from GOV.UK and Endole.
- `scrape_gov_uk(company_name)`: Scrapes data from the GOV.UK website.
- `scrape_endole_search(company_name)`: Scrapes data from the Endole search page.
- `scrape_endole_detail(crn, company_name)`: Scrapes data from the Endole detail page.

## Contributing

1. Fork the repository.
2. Create a new branch for your feature or bug fix.
3. Make your changes and commit them.
4. Push your branch to your forked repository.
5. Submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).

## Testing

No formal testing framework is currently set up for this project. Manual testing has been performed during development.