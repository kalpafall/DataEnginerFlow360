# Contributing to DataEnginerFlow360

Thank you for your interest in contributing to DataEnginerFlow360! 🎉

## How to Contribute

### Reporting Issues

- Use GitHub Issues to report bugs
- Provide detailed information about the issue
- Include steps to reproduce

### Pull Requests

1. Fork the repository
2. Create a new branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Commit your changes (`git commit -m 'Add amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

### Code Style

- Follow PEP 8 for Python code
- Use meaningful variable names
- Add comments for complex logic
- Write tests for new features

### Testing

- Run existing tests before submitting PR
- Add tests for new features
- Ensure dbt tests pass: `dbt test`

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/DataEnginerFlow360.git

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run tests
python3 test_pipeline.py
```

## Questions?

Feel free to open an issue for any questions!
