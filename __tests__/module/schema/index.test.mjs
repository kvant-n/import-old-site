
import expect from 'expect'

import chalk from "chalk";

import {
  verifySchema,
} from "../../default/schema.test.mjs";

import TestModule from "../../../";


import mocha from 'mocha'
const { describe, it } = mocha

const module = new TestModule();


/**
 */

const requiredTypes = [
  {
    name: "Import",
    fields: {
      both: [
        "id",
        "name",
        "status",
        "Logs",
        "CreatedBy",
      ],
      prisma: [
      ],
      api: [
      ],
    },
  },
  {
    name: "Log",
    fields: {
      both: [
        "id",
        "Import",
      ],
      prisma: [
      ],
      api: [
      ],
    },
  },
  {
    name: "User",
    fields: {
      both: [
        "id",
        "image",
        "oldID",
        "Imports",
        "Votes",
        "NotificationTypes",
        "Tags",
        "ResourceTags",
      ],
      prisma: [
      ],
      api: [
      ],
    },
  },
  {
    name: "Resource",
    fields: {
      both: [
        "id",
        "oldID",
        "published",
        "deleted",
        "hidemenu",
        "searchable",
        "Topics",
        "Blog",
        "Tags",
        "class_key",
        "template",
      ],
      prisma: [
      ],
      api: [
      ],
    },
  },
  {
    name: "NotificationType",
    fields: {
      both: [
        "id",
        "oldID",
        "CreatedBy",
      ],
      prisma: [
      ],
      api: [
      ],
    },
  },
  {
    name: "Project",
    fields: {
      both: [
        "id",
        "oldID",
        "image",
        "Resource",
      ],
      prisma: [
      ],
      api: [
      ],
    },
  },
  {
    name: "Service",
    fields: {
      both: [
        "id",
        "oldID",
        "Resource",
      ],
      prisma: [
      ],
      api: [
      ],
    },
  },
]




describe('modxclub Verify prisma Schema', () => {

  verifySchema(module.getSchema(), requiredTypes);

});


describe('modxclub Verify API Schema', () => {

  verifySchema(module.getApiSchema(), requiredTypes);

});