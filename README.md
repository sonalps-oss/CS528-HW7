# CS528 Homework 7

Apache Beam pipeline for analyzing 12,000 HTML files.

The pipeline calculates:
1. Top 5 files by incoming links
2. Top 5 files by outgoing links
3. Top 5 most frequent word trigrams

Execution environments:
- Apache Beam FnApiRunner (local)
- Google Cloud DataflowRunner
