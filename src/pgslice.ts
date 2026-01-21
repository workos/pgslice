import {
  CommonQueryMethods,
  createPool,
  DatabaseTransactionConnection,
  type DatabasePool,
} from "slonik";

interface PgsliceOptions {
  dryRun?: boolean;
}

export class Pgslice {
  #dryRun: boolean;

  constructor(
    connection: DatabasePool | CommonQueryMethods,
    options: PgsliceOptions,
  ) {
    this.#dryRun = options.dryRun ?? false;
    this.#connection = connection;
  }

  static async connect(
    databaseUrl: URL,
    options: PgsliceOptions = {},
  ): Promise<Pgslice> {
    const connection = await createPool(databaseUrl.toString());
    const instance = new Pgslice(connection, options);
    return instance;
  }

  #connection: DatabasePool | CommonQueryMethods | null = null;

  async start<T>(
    handler: (transaction: DatabaseTransactionConnection) => Promise<T>,
  ): Promise<T> {
    if (!this.#connection) {
      throw new Error("Not connected to the database");
    }

    if (this.#dryRun) {
      throw new Error("Dry run not yet supported.");
    }

    return this.#connection.transaction(handler);
  }

  async close(): Promise<void> {
    if (this.#connection) {
      if ("end" in this.#connection) {
        await this.#connection.end();
      }
      this.#connection = null;
    }
  }
}
