# Apple Music Feed Python Example

Script for how to get urls from the latest dump for Apple Music Feed, store them as files, and then load them in parquet
and analyze them.

## Running:
Designed for Python 3.7
Use a VirtualEnv.

Create the virtual env with
```bash
python3 -m venv .venv
source .venv/bin/activate
```

then install dependencies with
```bash
python3 -m pip install -r requirements.txt
```

run with
```bash
python3 apple_music_feed_example.py
```

use the following command to get help describing each parameter:
```bash
python3 apple_music_feed_example.py --help
```

## Unit Tests

Run the unit tests by running (following the above instructions for installing dependencies)
```bash
python3 test_apple_music_feed_example.py
```

