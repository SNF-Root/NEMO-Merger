# NEMO-Merger

A comprehensive toolset for migrating SNSF (Stanford Nanofabrication Facility) data to the NEMO (Nano Environment Management and Operations) system. This project provides scripts to create accounts, projects, and tools in NEMO based on data from SNSF Excel files.

## Overview

The NEMO-Merger project consists of several Python scripts that work together to:

1. **Download rate categories** from NEMO API
2. **Download existing accounts** from NEMO API  
3. **Create new accounts** with proper rate categories
4. **Create new projects** linked to accounts
5. **Push tools** to NEMO API

## Prerequisites

- Python 3.7+
- Access to NEMO API with valid authentication token
- SNSF data files in Excel format

## Installation

1. Clone the repository:
```bash
git clone https://github.com/SNF-Root/NEMO-Merger.git
cd NEMO-Merger
```

2. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install pandas openpyxl requests python-dotenv
```

4. Create a `.env` file with your NEMO API token:
```bash
echo "NEMO_TOKEN=your_actual_nemo_token_here" > .env
```

## Configuration

### Environment Variables

Create a `.env` file in the project root with:

```env
NEMO_TOKEN=your_nemo_api_token_here
```

### Data Files

Place your SNSF data files in the `SNSF-Data/` directory:
- `User Information.xlsx` - Contains PI information, project details, and account types
- `SNC Tools.xlsx` - SNC facility tools
- `SNL Tools.xlsx` - SNL facility tools  
- `SMF Tools.xlsx` - SMF facility tools

## Usage

### Step 1: Download Rate Categories

First, download the rate categories from NEMO to create the proper mapping:

```bash
python3 download_rate_categories.py
```

This creates:
- `nemo_rate_categories.json` - Raw rate category data
- `rate_category_mapping.json` - Mapping from Excel types to NEMO rate category IDs

### Step 2: Download Existing Accounts

Download existing accounts from NEMO (optional, but recommended):

```bash
python3 download_accounts.py
```

This creates:
- `nemo_accounts.json` - Raw account data
- `account_lookup.json` - Mapping from account names to IDs

### Step 3: Create Accounts

Create new accounts in NEMO based on PI information:

```bash
python3 create_accounts.py
```

The script will:
- Read PI information from `User Information.xlsx`
- Map account types to rate categories (local→Academic, industrial→Industry, etc.)
- Create accounts via NEMO API

### Step 4: Create Projects

Create new projects in NEMO linked to accounts:

```bash
python3 create_projects.py
```

The script will:
- Read project information from `User Information.xlsx`
- Match projects to accounts based on PI names
- Set proper rate categories based on PI types
- Create projects via NEMO API

### Step 5: Push Tools

Push tools from Excel files to NEMO:

```bash
python3 tool_push.py
```

The script will:
- Read tool names from all three facility Excel files
- Create tool entries with minimal required data
- Push tools via NEMO API

## Script Details

### download_rate_categories.py
Downloads rate categories from NEMO API and creates mapping for account types.

**Mappings:**
- `local` → Academic
- `industrial` → Industry
- `no charge` → No Charge
- `other academic` → Academic (default)
- `industrial-sbir` → Industry (default)
- `foreign` → Academic (default)

### download_accounts.py
Downloads existing accounts from NEMO API for reference and lookup.

### create_accounts.py
Creates new accounts in NEMO based on PI information from Excel files.

### create_projects.py
Creates new projects in NEMO, linking them to existing accounts and setting proper rate categories.

### tool_push.py
Pushes tools from Excel files to NEMO API with minimal required data.

## File Structure

```
NEMO-Merger/
├── SNSF-Data/                 # SNSF Excel data files
│   ├── User Information.xlsx
│   ├── SNC Tools.xlsx
│   ├── SNL Tools.xlsx
│   └── SMF Tools.xlsx
├── download_rate_categories.py # Download rate categories
├── download_accounts.py       # Download existing accounts
├── create_accounts.py         # Create new accounts
├── create_projects.py         # Create new projects
├── tool_push.py              # Push tools to NEMO
├── .env                      # Environment variables (not committed)
├── .gitignore               # Git ignore rules
└── README.md                # This file
```

## Generated Files

The scripts generate several JSON files (not committed to git):

- `nemo_rate_categories.json` - Rate categories from NEMO
- `rate_category_mapping.json` - Excel type to rate category mapping
- `nemo_accounts.json` - Accounts from NEMO
- `account_lookup.json` - Account name to ID mapping

## Error Handling

All scripts include comprehensive error handling:
- API connection testing
- Authentication validation
- Data validation and cleaning
- Detailed logging and progress reporting
- Graceful fallbacks for missing data

## API Endpoints

The scripts interact with these NEMO API endpoints:
- `https://nemo-plan.stanford.edu/api/billing/rate_categories/`
- `https://nemo-plan.stanford.edu/api/accounts/`
- `https://nemo-plan.stanford.edu/api/projects/`
- `https://nemo-plan.stanford.edu/api/tools/`

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions, please create an issue in the GitHub repository or contact the SNF team.

## Changelog

### v1.0.0
- Initial release
- Support for accounts, projects, and tools
- Rate category mapping
- Comprehensive error handling
- Excel data processing
