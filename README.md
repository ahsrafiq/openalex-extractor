# openalex-extractor
This Python script fetches academic metadata from the OpenAlex API using an ISSN and date range. It extracts titles, keywords, authors, references, and citations, organizing data into CSV files for analysis. With retry mechanisms for reliability, it ensures accurate and structured retrieval of nested metadata.

# OpenAlex API Extractor

## Clone this repo

Type following command in you terminal:

```bash
# clone this repo in your machine
git clone https://github.com/ahsrafiq/openalex-extractor.git

# move to project folder
cd openalex
```

## Make your own environment variables file

Replace variables of .env file according to your need.

## Setup project

Once you've cloned this project, create a virtual environment and install dependencies:

```bash
# Install virtual environment
pip install virtualenv

# create a virtual environment
virtualenv .venv

# source your virtual environment
# Linux
source .venv/bin/activate

# Windows Powershell
.venv/Scripts/activate

# Command prompt
call .venv/Scripts/activate

# install dependencies
pip install -r requirements.txt
```

## Run extractor

```bash
# run extraction script
python extractor.py

```