# ENVIRONMENT DETAILS
- **O/S**: MacOS Sonoma
- **Architecture**: Macbook Pro M1 Max
- **Terminal**: Zsh
- **Browser**: Chrome
Avoid responding with information related to other environments.

# OPERATIONAL FEATURES
- **Context Window Warnings**: Alert the user when nearing the context window limit.
- **Missing Content Requests**: Request the user provide project code, documentation, or definitions necessary for an adequate response.
- **Error Correction**: Indicate all user prompt errors of terminology, convention, or understanding, regardless of their relevance to the user prompt.
- **Use diff edits**: Always edit with diffs, i.e. editing only relevant parts of the code, not full rewrites of each file. (Full rewrites are both expensive and might lead to max output tokens reached errors.)

# CRITICALLY IMPORTANT RULES
1. **Completeness**: Generate full code, no placeholders. If unable, explain in comments.
2. **Comments**: Include clear inline comments and Python docstrings for functions, classes and methods.

# PYTHON ENVIRONMENT
1. **Types**: Use Python type hints when feasible, in particular on functions and methods. Use `pyright` for type checking.
2. **Testing**: All unit tests should be written with `pytest`. Always prefer the `pytest` suite of addons over the stdlib `unittest` modules.
3. **Python package manager**: Use `uv` package manager for managing depenendencies. Do not use the `uv pip` legace syntax for installing/managing packages, but instead the recent `uv add <pip package name>` instead.

# PYTHON CODING CONVENTIONS
1. Prefer clear, verbose code over concise and potentially ambigous code.
2. Always use a comma after the last element in a list, tuple, function arguments etc.
3. Always use Pathlib for operating system paths, not simple strings.

