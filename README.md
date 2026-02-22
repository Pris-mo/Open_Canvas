# **Open Canvas**

Open Canvas is a Python library for ingesting and processing Canvas LMS course content into retrieval-ready Markdown chunks. It provides the core crawling, conversion, filtering, and chunking logic that powers the [Open_Canvas_Pipeline](https://github.com/Pris-mo/Open\_Canvas\_Pipeline) Docker setup.

At a high level, Open Canvas:

- Connects to Canvas using the REST API and a personal access token  
- Crawls student-visible course content (pages, files, assignments, discussions, etc.)  
- Converts HTML, PDF, and other files to Markdown using Docling and MarkItDown (with optional LLM fallback via GPT-4o)  
- Splits content into chunks using LangChain text splitters  
- Optionally filters out sensitive content such as exams or solutions

The library is intended for use inside a pipelines container or as a standalone tool for building Canvas-to-RAG workflows.

## **Installation**

Clone the repository and install it as an editable package:

\`\`\`bash  
git clone https://github.com/Pris-mo/Open_Canvas.git  
cd Open\_Canvas  
pip install \-e .

Python 3.10 or later is required.

## **Command-line usage**

The main entry point is the orchestrator CLI, which coordinates crawling, conversion, filtering, and chunking.

Basic example (CLI mode):

python \-m orchestrator.cli \\  
 \--course-url "https://your.canvas.instance/courses/1234" \\  
 \--canvas-token "\<YOUR\_CANVAS\_TOKEN\>" \\  
 \--include "course\_root" \\  
 \--steps "all"

By default this will:

* Create a run directory under `runs/`

* Crawl the specified course

* Convert supported files to Markdown

* Write chunked Markdown files to disk

You can also run the orchestrator in YAML mode by providing a pipeline configuration file:

python \-m orchestrator.cli \--config path/to/pipeline\_config.yml

## **Components**

The library is organized into several modules:

* `canvas_crawler`  
   Canvas API client and crawlers for different resource types, implemented using an abstract factory pattern.

* `pre_processer`  
   Document conversion pipeline that applies Docling, MarkItDown, and optional LLM-based conversion.

* `chunker`  
   Chunking utilities based on `langchain-text-splitters`, including recursive splitting for Markdown content.

* `filterer`  
   Optional filtering of crawled items using title-based blacklists, token thresholds, and deduplication.

* `orchestrator`  
   High-level pipeline runner that ties together crawling, conversion, filtering, and chunking, and exposes the CLI entry point.

## **Relationship to Open Canvas Pipeline**

This repository provides the core processing logic. The companion project [Open\_Canvas\_Pipeline](https://github.com/Pris-mo/Open_Canvas_Pipeline?utm_source=chatgpt.com) wraps Open Canvas in a Docker Compose environment with Open WebUI and a Canvas Course Provisioner pipeline, making it easier to deploy a full Canvas-to-chat workflow.

