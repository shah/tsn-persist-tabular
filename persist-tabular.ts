import * as pipe from "@shah/ts-pipe";
import * as fs from "fs";
import * as path from "path";
import { v5 as uuid } from "uuid";

export type UUID = string;

export interface PersistProperties {
    [name: string]: any;
}

export interface PersistPropsTransformContext<T> {
    readonly persist: PersistProperties;
    readonly source: T;
}

export interface PersistPropsTransformer extends pipe.PipeUnionSync<PersistPropsTransformContext<any>, PersistProperties> {
}

export interface TabularColumnDefn {
    delimitedHeader(): string;
    delimitedContent(pp: PersistProperties): string;
    sqlDdlCreateTableColumnClause(): string;
    sqlDdlCreateTableClause(): string | undefined;
}

export class GuessColumnDefn {
    constructor(readonly name: string, readonly guessedFrom: PersistProperties) {
    }

    delimitedHeader(): string {
        return this.name;
    }

    delimitedContent(pp: PersistProperties): string {
        const value = pp[this.name];
        return this.name == "id" || this.name.endsWith("_id")
            ? value
            : JSON.stringify(value);
    }

    isNumber(value: any): boolean {
        if (typeof value === "number") return true;
        if (!isNaN(value)) return true;
        if (typeof value != "string") return false;
        return !isNaN(parseFloat(value));
    }

    sqlDdlCreateTableColumnClause(indent: string = "    "): string {
        const guessValue = this.guessedFrom[this.name];
        if (this.isNumber(guessValue)) {
            if (Number.isInteger(guessValue)) {
                return `${indent}${this.name} INT`;
            }
            return `${indent}${this.name} NUMERIC(16,2)`;
        } else {
            return `${indent}${this.name} VARCHAR(8192)`;
        }
    }

    sqlDdlCreateTableClause(indent: string = "    "): string | undefined {
        return this.name == "id" ? `${indent}PRIMARY KEY (${this.name})` : undefined;
    }
}

export interface TabularWriterOptions {
    readonly destPath: string;
    readonly fileName: string;
    readonly parentUuidNamespace: string;
    readonly ppTransform?: PersistPropsTransformer;
    readonly schema?: TabularColumnDefn[];
}

export class TabularWriter<T> {
    readonly columnDelim = ",";
    readonly recordDelim = "\n";
    readonly destPath: string;
    readonly fileName: string;
    readonly pkNamespace: UUID;
    readonly schema: TabularColumnDefn[];
    readonly ppTransform?: PersistPropsTransformer;
    readonly csvStream: fs.WriteStream;
    protected rowIndex: number = 0;

    constructor({ destPath, fileName, parentUuidNamespace, ppTransform, schema }: TabularWriterOptions) {
        this.destPath = destPath;
        this.fileName = fileName;
        this.csvStream = fs.createWriteStream(path.join(destPath, fileName));
        this.schema = schema || [];
        this.ppTransform = ppTransform;
        this.pkNamespace = uuid(fileName, parentUuidNamespace);
    }

    createId(name: string): UUID {
        return uuid(name, this.pkNamespace);
    }

    close(): void {
        this.csvStream.close();
    }

    guessSchema(guessFrom: PersistProperties): void {
        if (this.schema.length == 0) {
            for (const name of Object.keys(guessFrom)) {
                this.schema.push(new GuessColumnDefn(name, guessFrom));
            }
        }
    }

    writeDelimitedHeader(guess: PersistProperties): void {
        this.guessSchema(guess);
        const headers: string[] = [];
        for (const column of this.schema) {
            headers.push(column.delimitedHeader());
        }
        this.csvStream.write(headers.join(this.columnDelim));
    }

    write(ctx: PersistPropsTransformContext<T>): boolean {
        let persist = ctx.persist;
        if (this.ppTransform) {
            persist = this.ppTransform.flow(ctx, persist);
        }
        if (persist) {
            if (this.rowIndex == 0) {
                this.writeDelimitedHeader(persist);
            }
            const content: string[] = [];
            for (const column of this.schema) {
                content.push(column.delimitedContent(persist));
            }
            this.csvStream.write(this.recordDelim);
            this.csvStream.write(content.join(this.columnDelim));
            this.rowIndex++;
            return true;
        }
        return false;
    }

    sqlDdlCreateTable(): string {
        const columns = this.schema.map(c => c.sqlDdlCreateTableColumnClause()).join(",\n");
        const tableMeta = this.schema.map(c => c.sqlDdlCreateTableClause()).join(",\n");
        return `    CREATE TABLE (\n${columns}\n${tableMeta}\n);`;
    }
}
