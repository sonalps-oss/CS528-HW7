
import re
from html.parser import HTMLParser
from pathlib import PurePosixPath


class PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.links = []
        self.text_parts = []
        self.ignored_depth = 0

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()

        if tag in ("script", "style"):
            self.ignored_depth += 1
            return

        if self.ignored_depth:
            return

        if tag == "a":
            for key, value in attrs:
                if key.lower() == "href" and value:
                    self.links.append(value)

    def handle_endtag(self, tag):
        if tag.lower() in ("script", "style"):
            self.ignored_depth = max(
                0, self.ignored_depth - 1
            )

    def handle_data(self, data):
        if not self.ignored_depth:
            self.text_parts.append(data)


def parse_html(html):
    parser = PageParser()
    parser.feed(html)
    parser.close()
    return parser.links, " ".join(parser.text_parts)


def extract_words(text):
    return re.findall(r"[a-z]+", text.lower())


def extract_trigrams(text):
    words = extract_words(text)
    return [
        (words[i], words[i + 1], words[i + 2])
        for i in range(len(words) - 2)
    ]


def resolve_link(source_filename, href):
    """Resolve same-directory HTML links."""
    href = href.split("#", 1)[0].split("?", 1)[0]
    if not href or "://" in href or href.startswith("/"):
        return None

    target = PurePosixPath(href)
    if len(target.parts) != 1:
        return None

    if not re.fullmatch(r"\d+\.html", target.name):
        return None

    return target.name
