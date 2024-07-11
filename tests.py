import unittest
from main import Processor, TDocument

class TestProcessor(unittest.TestCase):
    def setUp(self):
        self.processor = Processor(":memory:")

    def test_single(self):
        doc = TDocument(url="http://example.com/doc1", pub_date=10, fetch_time=20, text="First version")
        result = self.processor.process(doc)

        self.assertEqual(result.url, "http://example.com/doc1")
        self.assertEqual(result.pub_date, 10)
        self.assertEqual(result.fetch_time, 20)
        self.assertEqual(result.text, "First version")
        self.assertEqual(result.first_fetch_time, 20)

    def test_multiple(self):
        docs = [
            TDocument(url="http://example.com/doc1", pub_date=10, fetch_time=20, text="First version"),
            TDocument(url="http://example.com/doc1", pub_date=10, fetch_time=30, text="Second version"),
            TDocument(url="http://example.com/doc1", pub_date=10, fetch_time=25, text="Third version"),
        ]

        for doc in docs:
            res = self.processor.process(doc)

        self.assertEqual(res.url, "http://example.com/doc1")
        self.assertEqual(res.pub_date, 10)
        self.assertEqual(res.fetch_time, 30)
        self.assertEqual(res.text, "Second version")
        self.assertEqual(res.first_fetch_time, 20)

    def test_different_urls(self):
        docs = [
            TDocument(url="http://example.com/doc1", pub_date=10, fetch_time=20, text="First version doc1"),
            TDocument(url="http://example.com/doc2", pub_date=15, fetch_time=25, text="First version doc2"),
            TDocument(url="http://example.com/doc1", pub_date=10, fetch_time=30, text="Second version doc1"),
        ]

        for doc in docs:
            result = self.processor.process(doc)

        result_doc1 = self.processor.process(docs[0])
        result_doc2 = self.processor.process(docs[1])

        self.assertEqual(result_doc1.url, "http://example.com/doc1")
        self.assertEqual(result_doc1.pub_date, 10)
        self.assertEqual(result_doc1.fetch_time, 30)
        self.assertEqual(result_doc1.text, "Second version doc1")
        self.assertEqual(result_doc1.first_fetch_time, 20)

        self.assertEqual(result_doc2.url, "http://example.com/doc2")
        self.assertEqual(result_doc2.pub_date, 15)
        self.assertEqual(result_doc2.fetch_time, 25)
        self.assertEqual(result_doc2.text, "First version doc2")
        self.assertEqual(result_doc2.first_fetch_time, 25)

    def test_same_fetch_time(self):
        docs = [
            TDocument(url="http://example.com/doc1", pub_date=10, fetch_time=20, text="First version"),
            TDocument(url="http://example.com/doc1", pub_date=10, fetch_time=20, text="Updated version"),
        ]

        for doc in docs:
            res = self.processor.process(doc)

        self.assertEqual(res.url, "http://example.com/doc1")
        self.assertEqual(res.pub_date, 10)
        self.assertEqual(res.fetch_time, 20)
        self.assertEqual(res.text, "Updated version")
        self.assertEqual(res.first_fetch_time, 20)

if __name__ == '__main__':
    unittest.main()
