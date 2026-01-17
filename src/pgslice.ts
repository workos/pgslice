import { createPool, type DatabasePool } from "slonik";

interface PgsliceOptions {
  dryRun?: boolean;
}

export class Pgslice {
  #dryRun: boolean;

  private constructor(
    readonly databaseUrl: URL,
    options: PgsliceOptions,
  ) {
    this.#dryRun = options.dryRun ?? false;
  }

  static async connect(
    databaseUrl: URL,
    options: PgsliceOptions,
  ): Promise<Pgslice> {
    const instance = new Pgslice(databaseUrl, options);
    await instance.initialize();
    return instance;
  }

  private async initialize(): Promise<void> {
    this.#connection = await createPool(this.databaseUrl.toString());
  }

  #connection: DatabasePool | null = null;

  get connection(): DatabasePool {
    if (!this.#connection) {
      throw new Error("Not connected to the database");
    }

    if (this.#dryRun) {
      throw new Error("Dry run not yet supported.");
    }

    return this.#connection;
  }
}
