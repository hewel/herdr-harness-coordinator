#!/usr/bin/env node

import { readFile, readdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const schemaRoot = join(repositoryRoot, "schemas");
const fixtureRoot = join(schemaRoot, "fixtures");
const schemaCache = new Map();

const fixtureSchemas = new Map([
  ["delivery-receipt", "delivery-receipt-v1.schema.json"],
  ["harness-definition", "harness-definition-v1.schema.json"],
  ["harness-launch-profile", "harness-launch-profile-v1.schema.json"],
  ["message-submission", "message-submission-v1.schema.json"],
  ["repository-observation", "repository-observation-v1.schema.json"],
  ["result-manifest", "result-manifest-v1.schema.json"],
  ["task-submission", "task-submission-v1.schema.json"],
]);

async function readJson(path) {
  return JSON.parse(await readFile(path, "utf8"));
}

async function loadSchema(path) {
  const absolute = resolve(path);
  if (!schemaCache.has(absolute)) {
    schemaCache.set(absolute, await readJson(absolute));
  }
  return schemaCache.get(absolute);
}

function pointerValue(document, pointer) {
  if (!pointer) return document;
  return pointer
    .replace(/^\//, "")
    .split("/")
    .reduce(
      (value, token) => value[token.replaceAll("~1", "/").replaceAll("~0", "~")],
      document,
    );
}

async function resolveReference(reference, schemaPath) {
  const [filePart, fragment = ""] = reference.split("#", 2);
  const targetPath = filePart ? resolve(dirname(schemaPath), filePart) : schemaPath;
  const document = await loadSchema(targetPath);
  return { schema: pointerValue(document, fragment), schemaPath: targetPath };
}

function sameValue(left, right) {
  return JSON.stringify(left) === JSON.stringify(right);
}

function matchesType(value, type) {
  switch (type) {
    case "array": return Array.isArray(value);
    case "boolean": return typeof value === "boolean";
    case "integer": return Number.isInteger(value);
    case "null": return value === null;
    case "number": return typeof value === "number" && Number.isFinite(value);
    case "object": return value !== null && typeof value === "object" && !Array.isArray(value);
    case "string": return typeof value === "string";
    default: throw new Error(`unsupported schema type: ${type}`);
  }
}

function validFormat(value, format) {
  if (format === "uuid") {
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
  }
  if (format === "date-time") {
    return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)
      && !Number.isNaN(Date.parse(value));
  }
  throw new Error(`unsupported schema format: ${format}`);
}

async function validate(schema, value, schemaPath, instancePath = "$") {
  const errors = [];
  const add = (message) => errors.push(`${instancePath}: ${message}`);

  if (schema.$ref) {
    const target = await resolveReference(schema.$ref, schemaPath);
    return validate(target.schema, value, target.schemaPath, instancePath);
  }

  if (schema.type) {
    const types = Array.isArray(schema.type) ? schema.type : [schema.type];
    if (!types.some((type) => matchesType(value, type))) {
      add(`expected ${types.join(" or ")}`);
      return errors;
    }
  }

  if ("const" in schema && !sameValue(value, schema.const)) add(`must equal ${JSON.stringify(schema.const)}`);
  if (schema.enum && !schema.enum.some((entry) => sameValue(value, entry))) add("is not an allowed value");

  for (const child of schema.allOf ?? []) {
    errors.push(...await validate(child, value, schemaPath, instancePath));
  }
  if (schema.oneOf) {
    const results = await Promise.all(schema.oneOf.map((child) => validate(child, value, schemaPath, instancePath)));
    if (results.filter((result) => result.length === 0).length !== 1) add("must match exactly one alternative");
  }
  if (schema.not && (await validate(schema.not, value, schemaPath, instancePath)).length === 0) add("matches a forbidden shape");
  if (schema.if) {
    const conditionMatches = (await validate(schema.if, value, schemaPath, instancePath)).length === 0;
    const branch = conditionMatches ? schema.then : schema.else;
    if (branch) errors.push(...await validate(branch, value, schemaPath, instancePath));
  }

  if (typeof value === "string") {
    const length = Array.from(value).length;
    if (schema.minLength !== undefined && length < schema.minLength) add(`is shorter than ${schema.minLength} Unicode scalars`);
    if (schema.maxLength !== undefined && length > schema.maxLength) add(`is longer than ${schema.maxLength} Unicode scalars`);
    if (schema.pattern && !new RegExp(schema.pattern, "u").test(value)) add(`does not match ${schema.pattern}`);
    if (schema.format && !validFormat(value, schema.format)) add(`is not a valid ${schema.format}`);
  }

  if (typeof value === "number" && schema.minimum !== undefined && value < schema.minimum) add(`is less than ${schema.minimum}`);

  if (Array.isArray(value)) {
    if (schema.minItems !== undefined && value.length < schema.minItems) add(`has fewer than ${schema.minItems} items`);
    if (schema.maxItems !== undefined && value.length > schema.maxItems) add(`has more than ${schema.maxItems} items`);
    if (schema.uniqueItems && new Set(value.map(JSON.stringify)).size !== value.length) add("contains duplicate items");
    if (schema.items) {
      for (const [index, item] of value.entries()) {
        errors.push(...await validate(schema.items, item, schemaPath, `${instancePath}[${index}]`));
      }
    }
  }

  if (value !== null && typeof value === "object" && !Array.isArray(value)) {
    for (const name of schema.required ?? []) {
      if (!(name in value)) add(`is missing required property ${name}`);
    }
    for (const [name, propertyValue] of Object.entries(value)) {
      if (schema.properties?.[name]) {
        errors.push(...await validate(schema.properties[name], propertyValue, schemaPath, `${instancePath}.${name}`));
      } else if (schema.additionalProperties === false) {
        add(`contains unknown property ${name}`);
      }
    }
  }

  return errors;
}

async function jsonFiles(path) {
  const entries = await readdir(path, { withFileTypes: true });
  const files = await Promise.all(entries.map(async (entry) => {
    const child = join(path, entry.name);
    return entry.isDirectory() ? jsonFiles(child) : (entry.name.endsWith(".json") ? [child] : []);
  }));
  return files.flat();
}

async function checkFixture(path, schemaName, shouldPass) {
  const schemaPath = join(schemaRoot, fixtureSchemas.get(schemaName));
  const errors = await validate(await loadSchema(schemaPath), await readJson(path), schemaPath);
  if (shouldPass && errors.length > 0) throw new Error(`${path} should pass:\n  ${errors.join("\n  ")}`);
  if (!shouldPass && errors.length === 0) throw new Error(`${path} should fail but passed`);
}

for (const [fixtureDirectory, schemaFile] of fixtureSchemas) {
  await loadSchema(join(schemaRoot, schemaFile));
  for (const fixture of await jsonFiles(join(fixtureRoot, fixtureDirectory))) {
    await checkFixture(fixture, fixtureDirectory, true);
  }
  const invalidDirectory = join(fixtureRoot, "invalid", fixtureDirectory);
  try {
    for (const fixture of await jsonFiles(invalidDirectory)) {
      await checkFixture(fixture, fixtureDirectory, false);
    }
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
}

await loadSchema(join(schemaRoot, "harness-launch-profile-v2.schema.json"));
await loadSchema(join(schemaRoot, "harness-launch-profile-v3.schema.json"));

const routeFixture = await readJson(join(fixtureRoot, "semantic-invalid", "worker-to-worker-route.invalid.json"));
const authenticatedSenderOwnsSubmission = !("from" in routeFixture.submission);
const routeAllowed = routeFixture.authenticated_sender.tier === "supervisor"
  || routeFixture.recipient.tier === "supervisor";
if (!authenticatedSenderOwnsSubmission || routeAllowed) {
  throw new Error("worker-to-worker semantic fixture did not exercise the identity-bound star-route rejection");
}

console.log(`Contract validation passed: ${schemaCache.size} schemas and all positive, negative, and semantic fixtures.`);
