import argparse
import sys
from oxidedb.database import Database


def main():
    parser = argparse.ArgumentParser(description="OxideDB CLI")
    subparsers = parser.add_subparsers(dest="command")

    get_parser = subparsers.add_parser("get", help="Get a value by key")
    get_parser.add_argument("key", help="The key to retrieve")

    set_parser = subparsers.add_parser("set", help="Set a key-value pair")
    set_parser.add_argument("key", help="The key")
    set_parser.add_argument("value", help="The value")

    delete_parser = subparsers.add_parser("delete", help="Delete a key")
    delete_parser.add_argument("key", help="The key to delete")

    scan_parser = subparsers.add_parser("scan", help="Scan keys in range")
    scan_parser.add_argument("start_key", help="Start key (inclusive)")
    scan_parser.add_argument("end_key", help="End key (exclusive)")

    args = parser.parse_args()

    db = Database()

    if args.command == "get":
        result = db.get(args.key.encode())
        if result is not None:
            print(result.decode())
        else:
            print("Key not found")
            sys.exit(1)
    elif args.command == "set":
        db.set(args.key.encode(), args.value.encode())
        print("OK")
    elif args.command == "delete":
        db.delete(args.key.encode())
        print("OK")
    elif args.command == "scan":
        results = db.scan(args.start_key.encode(), args.end_key.encode())
        for key, value in results:
            print(f"{key.decode()}: {value.decode()}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
