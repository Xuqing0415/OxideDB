from oxidedb.database import Database


def main():
    db = Database()

    print("=== Basic Key-Value Operations ===")
    db.set(b"user:1:name", b"Alice")
    db.set(b"user:1:age", b"30")
    db.set(b"user:2:name", b"Bob")

    print(f"user:1:name = {db.get(b'user:1:name').decode()}")
    print(f"user:1:age = {db.get(b'user:1:age').decode()}")
    print(f"user:2:name = {db.get(b'user:2:name').decode()}")

    print("\n=== Scan Operation ===")
    users = db.scan(b"user:", b"user:3")
    for key, value in users:
        print(f"{key.decode()}: {value.decode()}")

    print("\n=== Transaction Example ===")
    txn = db.begin()
    txn.set(b"user:1:name", b"Alice Updated")
    txn.delete(b"user:2:name")
    txn.set(b"user:3:name", b"Charlie")
    txn.commit()

    print(f"user:1:name after transaction = {db.get(b'user:1:name').decode()}")
    print(f"user:2:name exists? {db.get(b'user:2:name') is not None}")
    print(f"user:3:name = {db.get(b'user:3:name').decode()}")

    print("\n=== Rollback Example ===")
    txn2 = db.begin()
    txn2.set(b"user:1:name", b"Should Be Rolled Back")
    print(f"user:1:name inside txn = {txn2.get(b'user:1:name').decode()}")
    txn2.rollback()
    print(f"user:1:name after rollback = {db.get(b'user:1:name').decode()}")


if __name__ == "__main__":
    main()
