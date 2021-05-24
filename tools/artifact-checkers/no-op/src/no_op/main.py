import sys


def run():
    print("🎉")
    with open("no-op-was-here", "w") as f:
        f.write(" ".join(sys.argv[1:]))


if __name__ == "__main__":
    run()
