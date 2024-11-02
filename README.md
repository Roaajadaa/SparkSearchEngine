# SparkSearchEngine

## Description

Build a small-scale spark-based search engine which searches in a list of documents to find those answering a user’s query. 


## Key Features

### Inverted Index Construction
- The project reads the content of multiple documents and generates an inverted index that records each unique word, the count of documents containing that word, and a sorted list of those documents.

### Data Storage
- The resulting inverted index is saved in a file (`wholeInvertedIndex.txt`) and subsequently stored in a MongoDB collection named "dictionary" for efficient retrieval.

### Query Processing
- Users can input queries to search for specific words or phrases. The system retrieves and displays the relevant document identifiers, showcasing the documents that match the search criteria.

### Sorting and Organization
- Both the words in the index and the associated document lists are sorted alphabetically and in ascending order, ensuring clarity and ease of use.

## Conclusion
This project combines big data processing techniques with information retrieval principles, providing a practical application of Spark and MongoDB in building scalable search solutions.

