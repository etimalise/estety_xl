from pathlib import Path
from utils import dotenv as env, secrets as sec

def main() -> None:
    print("estety-xl runs")
    print(env, sec)


if __name__ == "__main__":
    main()
