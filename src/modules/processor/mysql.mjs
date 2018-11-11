
import Knex from "knex";

export class MySQL {


  constructor(config) {

    Object.assign(this, {
      config,
    });

    const {
      host,
      database,
      user,
      password,
    } = config;

    let knexOptions = {
      client: 'mysql',
      connection: {
        host,
        user,
        database,
        password,
      },
    }

    this.knex = Knex(knexOptions);

  }

  getKnex() {

    return this.knex;
  }


  getConfig() {
    return this.config || {}
  }


  getTablePrefix() {

    const {
      tablePrefix,
    } = this.getConfig();

    return tablePrefix;
  }


  getTableName(tableName, alias) {

    const {
      // host,
      database,
    } = this.getConfig();

    const tablePrefix = this.getTablePrefix();

    if (!tableName) {
      throw new Error("tableName is empty");
    }

    let segment = tableName.trim().split(".");

    segment.unshift(database);
    // segment.unshift(host);

    let name = segment[segment.length - 1];

    if (alias !== null) {
      alias = alias || name;
    }

    if (tablePrefix) {
      name = `${tablePrefix}${name}`;
      segment[segment.length - 1] = name;
    }

    tableName = segment.join(".");


    if (alias) {
      tableName += ` as ${alias}`;
    }

    return tableName;
  }


  debugQuery(query) {

    const tablePrefix = this.getTablePrefix();

    return query.toString().replace(new RegExp(tablePrefix, 'g'), '');
  }



  getQuery(tableName, alias, args = {}, ctx, query) {

    const knex = this.getKnex();

    let {
      first,
      skip,
      where,
      orderBy,
    } = args;


    if (!query) {
      query = knex(this.getTableName(tableName, alias));
    }


    const tableAlias = alias || tableName;


    this.where(query, where, tableAlias);


    if (orderBy) {

      let match = orderBy.match(/^(.+)\_(ASC|DESC)$/);

      let by;
      let dir;

      if (match) {
        by = match[1];
        dir = match[2].toLowerCase();
      }
      else {
        by = orderBy;
      }

      query.orderBy(`${tableAlias}.${by}`, dir);

    }


    if (first) {
      query.limit(first);
    }

    if (skip) {
      query.offset(skip);
    }

    return query

  }


  where(query, argsWhere, tableAlias, OR = false) {

    let where = {}


    query[OR ? "orWhere" : "andWhere"](builder => {


      for (var field in argsWhere) {

        let condition = argsWhere[field];

        if (field === "OR") {

          builder.andWhere(builder => {

            condition.map(n => {

              this.where(builder, n, tableAlias, true);

            });

          });

          continue;
        }


        let whereNotInMatch = field.match(/(.*)\_not_in$/);

        if (whereNotInMatch) {

          const field = `${tableAlias}.${whereNotInMatch[1]}`;

          builder.orWhereNotIn(field, condition);

          continue;
        }

        let whereInMatch = field.match(/(.*)\_in$/);

        if (whereInMatch) {

          const field = `${tableAlias}.${whereInMatch[1]}`;

          builder.orWhereIn(field, condition);
          continue;
        }

        // else 
        where[`${tableAlias}.${field}`] = condition;

      }

      return builder.where(where);

    });

    return query;


  }


  async request(query) {

    return await query
      .catch(error => {

        console.error(chalk.red("SQL error"), error);

        throw new Error("SQL Error");

      });
  }

}

export default MySQL;