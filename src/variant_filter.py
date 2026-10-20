import re


MOBILE_PATTERN = re.compile(
    r"\bandroid\b"
    r"|\bios\b"
    r"|\bflutter\b"
    r"|react[\s-]*native"
    r"|\bmobile\b"
    r"|мобильн\w*",
    re.IGNORECASE,
)

DEVELOPER_PATTERN = re.compile(
    r"разработ\w*"
    r"|программист\w*"
    r"|\bdeveloper\b"
    r"|\bsoftware engineer\b"
    r"|\btech lead\b",
    re.IGNORECASE,
)

EXCLUDE_PATTERN = re.compile(
    r"\bqa\b"
    r"|\btester\b"
    r"|тестиров\w*"
    r"|аналитик\w*"
    r"|\banalyst\b"
    r"|дизайнер\w*"
    r"|\bdesigner\b"
    r"|продавец\w*"
    r"|маркетолог\w*"
    r"|sales"
    r"|руководитель"
    r"|стажер-испытатель",
    re.IGNORECASE,
)


def is_mobile_developer(title):
    """Проверяет соответствие вакансии варианту №9."""
    if not isinstance(title, str):
        return False

    title = title.strip()

    return (
        bool(MOBILE_PATTERN.search(title))
        and bool(DEVELOPER_PATTERN.search(title))
        and not bool(EXCLUDE_PATTERN.search(title))
    )


def normalize_skills(raw_skills):
    """Нормализует и удаляет повторы навыков внутри вакансии."""
    if not isinstance(raw_skills, str):
        return []

    skills = set()

    for skill in raw_skills.split(","):
        skill = skill.strip().casefold()

        if skill:
            skills.add(skill)

    return sorted(skills)
