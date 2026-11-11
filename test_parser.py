
import unittest

from html_parser import (
    parse_html,
    extract_trigrams,
    resolve_link,
)


class TestHTMLParser(unittest.TestCase):

    def test_links(self):
        html = """
        <html><body>
          <a HREF="332.html">Link One</a>
          <a href="4736.html">Link Two</a>
        </body></html>
        """
        links, text = parse_html(html)

        self.assertEqual(
            links, ["332.html", "4736.html"]
        )
        self.assertIn("Link One", text)

    def test_trigrams(self):
        result = extract_trigrams(
            "Lorem ipsum dolor sit amet"
        )
        self.assertEqual(
            result,
            [
                ("lorem", "ipsum", "dolor"),
                ("ipsum", "dolor", "sit"),
                ("dolor", "sit", "amet"),
            ],
        )

    def test_relative_link(self):
        self.assertEqual(
            resolve_link("1654.html", "332.html"),
            "332.html",
        )

    def test_ignore_scripts(self):
        html = """
        <script>secret hidden words</script>
        <p>Hello world again</p>
        """
        _, text = parse_html(html)
        self.assertNotIn("secret", text)
        self.assertIn("Hello world again", text)


if __name__ == "__main__":
    unittest.main()
