# Web Company Data Scraper 🏢

This project is a web scraper designed to extract company information from the UK Companies House website (GOV.UK) and the Endole website. It takes a list of company names as input, searches for these companies on both platforms, and retrieves detailed information about each company, storing the results in an Excel file. The scraper is configured with delays and retry mechanisms to avoid being blocked by the websites. It also includes logic to clean and format the extracted data, making it a valuable tool for market research, competitive analysis, and data enrichment.

🚀 **Key Features**

*   **Dual-Source Scraping:** Extracts data from both GOV.UK and Endole for comprehensive company profiles. 🇬🇧
*   **Data Cleaning & Formatting:** Cleans and formats extracted data, including address standardization and sector categorization. ✨
*   **Configurable Delays:** Implements configurable delays between requests to avoid being blocked by websites. ⏳
*   **Retry Mechanism:** Includes retry mechanisms for failed requests to handle temporary website errors. 🔄
*   **Cloudflare Bypass:** Uses `cloudscraper` to bypass Cloudflare's anti-bot protection. 🛡️
*   **Sector Categorization:** Categorizes companies based on keywords in their descriptions using a predefined mapping. 📊
*   **Excel Output:** Stores the scraped data in a well-structured Excel file for easy analysis and integration. 📊

🛠️ **Tech Stack**

*   **Frontend:** N/A (command-line tool)
*   **Backend:** Python 3.x
*   **Web Scraping:**
    *   `requests`: For making HTTP requests.
    *   `cloudscraper`: For bypassing Cloudflare's anti-bot protection.
    *   `BeautifulSoup4 (bs4)`: For parsing HTML content.
*   **Data Manipulation:** `pandas`: For data manipulation and storage in a dataframe.
*   **Data Storage:** Excel (`.xlsx`)
*   **Other:**
    *   `time`: For implementing delays.
    *   `random`: For generating random delays.
    *   `re`: For regular expression matching.
    *   `datetime`: For handling date and time information.
    *   `logging`: For logging errors and debugging information.

📦 **Getting Started**

### Prerequisites

*   Python 3.x installed
*   `pip` package manager

### Installation

1.  Clone the repository:

    ```bash
    git clone <repository_url>
    cd <repository_directory>
    ```

2.  Install the required Python packages:

    ```bash
    pip install requests beautifulsoup4 pandas cloudscraper openpyxl
    ```

### Running Locally

1.  Prepare your input CSV file (`company_list.csv`) with a list of company names.
2.  Configure the `INPUT_FILENAME` and `OUTPUT_FILENAME` variables in `scraper.py` to match your desired input and output file names.
3.  Run the scraper:

    ```bash
    python scraper.py
    ```

4.  The scraped data will be saved to the specified output Excel file (`company_data_filled.xlsx`).

📂 **Project Structure**

```
.
├── scraper.py           # Main script for web scraping
├── company_list.csv      # Input CSV file with company names
├── company_data_filled.xlsx # Output Excel file with scraped data
└── README.md             # Documentation
```


🤝 **Contributing**

Contributions are welcome! Please feel free to submit pull requests or open issues to suggest improvements or report bugs.

📝 **License**

[Specify the license under which your project is released. E.g., MIT License]

📬 **Contact**

[Your Name/Organization] - [Your Email/Website]

💖 **Thanks**

Thank you for using this web scraper! We hope it helps you in your data extraction endeavors.

This is written by [readme.ai](https://readme-generator-phi.vercel.app/).
