from urllib.robotparser import RobotFileParser


def test_robotparser_disallow():
    parser = RobotFileParser()
    parser.parse(["User-agent: *", "Disallow: /private"])
    assert parser.can_fetch("TestBot", "https://example.com/private/secret") is False
    assert parser.can_fetch("TestBot", "https://example.com/public") is True


def test_robotparser_allows_all_when_empty():
    parser = RobotFileParser()
    parser.parse([])
    assert parser.can_fetch("TestBot", "https://example.com/any") is True
