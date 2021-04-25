To run the script:

1. Install the python requirements by running the following command while in
the project directory.

        pip install -r requirements.txt

2. Set your api credentials to the appropriate location. The location will
vary depending on the TwitterApi  installation location. You can find the
location by running the script, the error should include the expected
credentials file location and format. You can run the script with the following
command.

        python search_full_archive_no_limits.py

    Example output before credentials are setup.

        [Errno 2] No such file or directory: '/Users/pavlov/.virtualenvs/twitter/lib/python3.8/site-packages/TwitterAPI/credentials.txt'

    The credentials file should follow the following format

        consumer_key=your_api_key
        consumer_secret=your_api_secret
        access_token_key=your_token_key
        access_token_secret=your_token_secret
