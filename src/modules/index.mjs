
import fs from "fs";

import chalk from "chalk";

import ImportProcessor from "./processor";

import LogModule from "@prisma-cms/log-module";
import UserModule from "@prisma-cms/user-module";
import ResourceModule from "@prisma-cms/resource-module";
import SocietyModule from "@prisma-cms/society-module";
import CooperationModule from "@prisma-cms/cooperation-module";

import PrismaModule from "@prisma-cms/prisma-module";

import MergeSchema from 'merge-graphql-schemas';

import path from 'path';

const moduleURL = new URL(import.meta.url);

const __dirname = path.dirname(moduleURL.pathname);

const { createWriteStream, unlinkSync } = fs;

const { fileLoader, mergeTypes } = MergeSchema

export {
  ImportProcessor,
}


class Module extends PrismaModule {


  constructor(props = {}) {

    super(props);

    this.mergeModules([
      LogModule,
      SocietyModule,
      UserModule,
      ResourceModule,
      CooperationModule,
    ]);

  }


  getSchema(types = []) {


    let schema = fileLoader(__dirname + '/schema/database/', {
      recursive: true,
    });


    if (schema) {
      types = types.concat(schema);
    }


    let typesArray = super.getSchema(types);

    return typesArray;

  }


  getApiSchema(types = []) {


    let baseSchema = [];

    let schemaFile = "src/schema/generated/prisma.graphql";

    if (fs.existsSync(schemaFile)) {
      baseSchema = fs.readFileSync(schemaFile, "utf-8");
    }

    let apiSchema = super.getApiSchema(types.concat(baseSchema), []);

    let schema = fileLoader(__dirname + '/schema/api/', {
      recursive: true,
    });

    apiSchema = mergeTypes([apiSchema.concat(schema)], { all: true });


    return apiSchema;

  }


  getResolvers() {

    const resolvers = super.getResolvers();


    Object.assign(resolvers.Query, {
      importsConnection: this.importsConnection,
      imports: this.imports,
      import: this.import,
    });

    Object.assign(resolvers.Mutation, {
      startImportProcessor: this.startImportProcessor.bind(this),
    });

    // Object.assign(resolvers.Subscription, this.Subscription);


    Object.assign(resolvers, {
      ImportResponse: this.ImportResponse(),
    });

    return resolvers;
  }


  import(source, args, ctx, info) {
    return ctx.db.query.import({}, info);
  }

  imports(source, args, ctx, info) {
    return ctx.db.query.imports({}, info);
  }

  importsConnection(source, args, ctx, info) {
    return ctx.db.query.importsConnection({}, info);
  }


  startImportProcessor(source, args, ctx, info) {

    return this.getProcessor(ctx).createWithResponse("Import", args, info);
  }


  getProcessor(ctx) {
    return new (this.getProcessorClass())(ctx);
  }

  getProcessorClass() {
    return ImportProcessor;
  }


  ImportResponse() {

    return {
      data: (source, args, ctx, info) => {

        const {
          id,
        } = source.data || {};

        return id ? ctx.db.query.import({
          where: {
            id,
          },
        }, info) : null;
      }
    }
  }

}


export default Module;