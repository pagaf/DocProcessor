import sqlite3
import argparse
from typing import Optional

class TDocument:
    def __init__(self, url: str, pub_date: int, fetch_time: int, text: str, first_fetch_time: Optional[int] = None):
        self.url = url
        self.pub_date = pub_date
        self.fetch_time = fetch_time
        self.text = text
        self.first_fetch_time = first_fetch_time if first_fetch_time is not None else fetch_time

    def __repr__(self):
        return f"TDocument(url={self.url}, pub_date={self.pub_date}, fetch_time={self.fetch_time}, text='{self.text}', first_fetch_time={self.first_fetch_time})"

class ProcessorInterface:
    def process(self, doc: TDocument) -> Optional[TDocument]:
        pass

class Processor(ProcessorInterface):
    def __init__(self, db_name: str = ":memory:"):
        self.conn = sqlite3.connect(db_name)
        self._create_table()

    def _create_table(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS documents (
                    url TEXT,
                    pub_date INTEGER,
                    fetch_time INTEGER,
                    text TEXT,
                    first_fetch_time INTEGER,
                    PRIMARY KEY (url, fetch_time)
                )
            """)

    def process(self, doc: TDocument) -> Optional[TDocument]:
        with self.conn:
            self.conn.execute("""
                INSERT OR REPLACE INTO documents (url, pub_date, fetch_time, text, first_fetch_time)
                VALUES (?, ?, ?, ?, ?)
            """, (doc.url, doc.pub_date, doc.fetch_time, doc.text, doc.first_fetch_time))

        return self._aggregate_documents(doc.url)

    def _aggregate_documents(self, url: str) -> TDocument:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM documents WHERE url = ? ORDER BY fetch_time", (url,))
        rows = cursor.fetchall()

        docs = [TDocument(*row) for row in rows]
        latest_doc = docs[-1]
        oldest_doc = docs[0]

        aggregated_doc = TDocument(
            url=url,
            pub_date=oldest_doc.pub_date,
            fetch_time=latest_doc.fetch_time,
            text=latest_doc.text,
            first_fetch_time=oldest_doc.first_fetch_time
        )
        return aggregated_doc

def main():
    parser = argparse.ArgumentParser(description="Document Processor CLI")
    parser.add_argument("--db", type=str, default=":memory:", help="Path to the SQLite database file")
    parser.add_argument("url", type=str, help="URL of the document")
    parser.add_argument("pub_date", type=int, help="Publication date of the document")
    parser.add_argument("fetch_time", type=int, help="Fetch time of the document")
    parser.add_argument("text", type=str, help="Text of the document")
    args = parser.parse_args()

    processor = Processor(args.db)
    doc = TDocument(url=args.url, pub_date=args.pub_date, fetch_time=args.fetch_time, text=args.text)
    result = processor.process(doc)

    print(f"Processed document: {result}")

if __name__ == "__main__":
    main()
