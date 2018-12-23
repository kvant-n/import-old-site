
import chalk from "chalk";
import PrismaProcessor from "@prisma-cms/prisma-processor";

import Knex from "knex";

import MySQL from "./mysql";

import {
  ResourceProcessor,
} from "@prisma-cms/resource-module";

import React from "react";
import Draft from "draft-js";


import jsdom from 'jsdom';

import fs from "fs";
import path from "path";
import mime from "mime";

// import URI from "urijs";
import punycode from "punycode";

const { JSDOM } = jsdom;


const w = (new JSDOM(`<!DOCTYPE html>`)).window;

const {
  document,
  HTMLElement,
  HTMLAnchorElement,
  HTMLImageElement,
} = w;

global.document = document;
global.HTMLElement = HTMLElement;
global.HTMLAnchorElement = HTMLAnchorElement;
global.HTMLImageElement = HTMLImageElement;


const {
  // CompositeDecorator,
  // ContentBlock,
  ContentState,
  // Editor,
  // EditorState,
  convertFromHTML,
  convertToRaw,
  // convertFromRaw,
} = Draft;


export default class ImportProcessor extends PrismaProcessor {


  constructor(props) {

    super(props);

    this.objectType = "Import";

    /**
     * Если выставлен флаг, то выполнение завершается
     */
    // this.stopped = false;

  }


  async create(method, args, info) {

    // console.log("create args", args);

    // return super.create(method, args, info);

    const {
      ctx,
    } = this;


    const {
      response: res,
    } = ctx;

    /**
     * Устанавливаем лимит на время запроса, иначе будет обрываться на 120 сек.
     */
    res && res.setTimeout(3600 * 1000, () => {
      console.error('Request has timed out.');
      // response.status(500);
      res.writeHead(408, 'Request Timeout');
      res.end('Timeout!')
    });

    const {
      currentUser,
      db,
    } = ctx;



    let {
      data,
      ...otherArgs
    } = args;

    data = data || {}

    let {
      name = "Импорт со старого сайта",
    } = data;

    /**
     * Проверяем, если есть запущенный импортер, возвращаем ошибку
     */
    if (await db.exists.Import({
      status: "Created",
      name,
    })) {
      return this.addError("Уже есть выполняемый импорт");
    }

    const {
      id: currentUserId,
    } = currentUser || {};

    // console.log("create currentUser", currentUser);

    Object.assign(data, {
      name,
      CreatedBy: currentUserId ? {
        connect: {
          id: currentUserId,
        },
      } : undefined,
    })



    let Import = await super.create(method, {
      data,
      otherArgs,
    });

    // console.log("create Import", Import);

    const {
      id: importId,
    } = Import || {};

    if (!importId) {
      return this.error(new Error("Не был получен ID импорта"));
    }

    this.Import = Import;

    let status = "Completed";

    // Выполняем импорт
    await this.processImport(args)
      .catch(error => {
        status = "Error";
        this.error(error);
      });

    let newData = {
      status,
    };

    return await this.update("Import", {
      where: {
        id: importId,
      },
      data: newData,
    });
  }




  async mutate(method, args, info) {

    // const {
    //   db,
    // } = this.ctx; 

    return super.mutate(method, args);
  }


  // async log(options, level = "Info") {

  //   await super.log(options, level = "Info");

  //   if(["Error", "Fatal"].indexOf(level) !== -1){
  //     throw new Error("Импорт завершился с ошибкой");
  //   }

  // }


  createLog(args) {

    const {
      id: importId,
    } = this.Import || {};

    if (importId) {
      Object.assign(args.data, {
        Import: {
          connect: {
            id: importId,
          },
        },
      });
    }

    return super.createLog(args);
  }


  async initDB(args) {

    // console.log("initDB args", args);

    const {
      sourceDbConfig,
      targetDbConfig,
    } = args;

    this.source = new MySQL(sourceDbConfig);
    this.target = new MySQL(targetDbConfig);

  }


  async * getProcessor(users, processor) {

    let writed = 0;
    let skiped = 0;
    let errors = 0;

    while (users && users.length) {

      const user = users.splice(0, 1)[0];

      const result = await processor(user)
        .catch(error => {
          errors++;
          this.error(error);
          return error;
        });

      if (result instanceof Error) {
        return;
      }

      if (result) {
        writed++;
      }
      else {
        skiped++;
      }

      yield result;
    }

    await this.log(`Записано: ${writed}, пропущено: ${skiped}, ошибок: ${errors}`, "Info");

    if (errors) {
      throw new Error("Есть ошибки при импорте");
    }

  }


  async processImport(args) {

    await this.initDB(args);

    // await this.importUserGroups();
    await this.importUsers();
    // await this.importBlogs();
    // await this.importTopics();
    // await this.importComments();
    // await this.importTags();
    // await this.importNotificationTypes();

    // await this.importTeams();
    // await this.importServices();
    // await this.importProjects();


  }

  /**
   * Import UserGroups
   */
  async importUserGroups() {

    this.log("Импортируем группы пользователей", "Info");

    // throw new Error("Test");

    const {
      source,
      target,
      ctx,
    } = this;

    const targetUserGroupsTable = target.getTableName("UserGroup", "targetUserGroup");


    const query = source.getQuery("membergroup_names", "userGroups")
      // .innerJoin(source.getTableName("userGroup_attributes", "profile"), "profile.internalKey", "userGroups.id")
      // .leftJoin(source.getTableName("society_userGroup_attributes"), "society_userGroup_attributes.internalKey", "userGroups.id")
      .leftJoin(targetUserGroupsTable, "targetUserGroup.oldID", "userGroups.id")
      ;

    query.whereNull("targetUserGroup.oldID");

    query.select([
      "userGroups.*",
      // "profile.fullname",
      // "profile.email",
      // "profile.photo as image",
      // // "society_userGroup_attributes.createdon as society_userGroup_createdon",
      // "userGroups.createdon as userGroup_createdon",
    ]);

    // query.limit(1);


    // console.log(chalk.green("query SQL"), query.toString());

    // return;

    const userGroups = await query.then();

    // console.log("userGroups", userGroups);

    await this.log(`Было получено ${userGroups && userGroups.length} групп пользователей`, "Info");

    const processor = this.getProcessor(userGroups, this.writeUserGroup.bind(this));

    for await (const result of processor) {

      // console.log("writeUserGroup result", result);

    }

  }



  async writeUserGroup(userGroup) {


    // console.log("writeUserGroup result this", this);

    const {
      ctx,
      target,
    } = this

    const {
      db,
    } = ctx;

    let result;

    const {
      id,
      name,
      description,
      parent,
    } = userGroup;

    /**
     * Сохраняем пользователя
     */
    result = await db.mutation.createUserGroup({
      data: {
        oldID: id,
        name,
        description,
        parent,
      },
    });


    return result;
  }

  /**
   * Eof Import UserGroups
   */


  /**
   * Import Users
   */
  async importUsers() {

    this.log("Импортируем пользователей", "Info");

    // throw new Error("Test");

    const {
      source,
      target,
      ctx,
    } = this;

    const knex = source.getKnex();

    const targetUsersTable = target.getTableName("User", "targetUser");


    const query = source.getQuery("users", "users")
      .innerJoin(source.getTableName("user_attributes", "profile"), "profile.internalKey", "users.id")
      .leftJoin(source.getTableName("member_groups"), "member_groups.member", "users.id")
      .leftJoin(targetUsersTable, "targetUser.oldID", "users.id")
      ;

    query.whereNull("targetUser.oldID");

    query.select([
      "users.*",
      "profile.fullname",
      "profile.email",
      "profile.photo as image",
      // "society_user_attributes.createdon as society_user_createdon",
      "users.createdon as user_createdon",
    ])
      .select(knex.raw("GROUP_CONCAT(DISTINCT member_groups.user_group) as group_ids"))
      ;

    query.groupBy("users.id");

    query.orderBy("users.active", "DESC");
    query.orderBy("profile.blocked", "ASC");

    query.where({
      // blocked: 1,
    });

    // query.limit(1);


    // console.log(chalk.green("query SQL"), query.toString());

    // return;

    const users = await query.then();

    // console.log("users", users);

    await this.log(`Было получено ${users && users.length} пользователей`, "Info");

    // return;

    const processor = this.getProcessor(users, this.writeUser.bind(this));

    for await (const result of processor) {

      // console.log("writeUser result", result);

    }

  }



  async writeUser(user) {


    // console.log("writeUser result this", this);

    const {
      ctx,
      target,
    } = this

    const {
      db,
    } = ctx;

    let result;

    let {
      id,
      username,
      fullname,
      email,
      society_user_createdon,
      user_createdon,
      image,
      active,
      blocked,

      group_ids,
    } = user;

    active = active === 1 && blocked !== 1 ? true : false;


    let groupIds = group_ids && group_ids.split(",").map(n => parseInt(n)) || [];

    let Groups;

    if (groupIds.length) {

      Groups = {
        connect: groupIds.map(oldID => ({
          oldID,
        })),
      }

    }


    /**
     * Так как некоторые емейлы не уникальные, проверяем сначала на наличие емейла.
     * Если его нет, то назначаем основной емейл.
     * Если есть, то назначаем oldEmail
     */
    let oldEmail;

    if (await db.exists.User({
      email,
    })) {

      oldEmail = email;
      email = undefined;

    }

    /**
     * Сохраняем пользователя
     */
    result = await db.mutation.createUser({
      data: {
        oldID: id,
        username,
        fullname,
        email,
        oldEmail,
        image,
        Groups,
        active,
      },
    });

    const {
      id: userId,
    } = result;

    /**
     * Если пользователь был сохранен, надо обновить дату его создания
     */
    let createdAt = society_user_createdon || user_createdon;
    createdAt = createdAt ? new Date(createdAt * 1000) : undefined;


    const query = target.getQuery("User", "users")

    if (createdAt) {

      await query.update({
        createdAt,
      })
        .where({
          id: userId,
        })
        .then();

    }


    // console.log(chalk.green("update query SQL"), query.toString());

    return result;
  }

  /**
   * Eof Import Users
   */


  /**
   * Import Blogs
   */
  async importBlogs() {

    this.log("Импортируем блоги", "Info");

    // throw new Error("Test");

    const {
      source,
      target,
      ctx,
    } = this;



    const query = source.getQuery("site_content", "source")
      ;

    query
      .leftJoin(target.getTableName("Resource", "target"), "target.oldID", "source.id")
      .innerJoin(target.getTableName("User"), "User.oldID", "source.createdby")
      .whereNull("target.id")
      .whereIn("source.template", [
        14,
        16,
      ])
      ;


    query.select([
      "source.*",
    ]);

    // query.limit(1);


    // console.log(chalk.green("query SQL"), query.toString());

    // return;

    const objects = await query.then();

    // console.log("objects", objects);

    await this.log(`Было получено ${objects && objects.length} блогов`, "Info");

    const processor = this.getProcessor(objects, this.writeBlog.bind(this));

    for await (const result of processor) {

      // console.log("writeUser result", result);

    }

  }


  async writeBlog(object) {

    const {
      ctx,
      target,
    } = this

    const {
      db,
    } = ctx;

    let result;

    let {
      id,
      pagetitle: name,
      createdon,
      editedon,
      createdby,
      uri,
      published,
      deleted,
      hidemenu,
      searchable,
      content: text,
      class_key,
      template,
    } = object;

    let type;

    switch (template) {
      case 14:
        type = "Blog";
        break;

      case 16:

        type = "PersonalBlog";
        break;

      default: throw new Error(`Wrong template ${template}`);
    }

    let {
      content,
      contentText,
    } = this.getContent(text) || {};

    uri = this.prepareUri(uri);

    /**
     * Сохраняем объект
     */
    result = await db.mutation.createResource({
      data: {
        type,
        oldID: id,
        uri,
        name,
        class_key,
        template,
        content,
        contentText,
        published: published === 1,
        deleted: deleted === 1,
        hidemenu: hidemenu === 1,
        searchable: searchable === 1,
        CreatedBy: {
          connect: {
            oldID: createdby,
          },
        },
      },
    });

    const {
      id: objectId,
    } = result;

    /**
     * Если пользователь был сохранен, надо обновить дату его создания
     */
    let createdAt = createdon ? new Date(createdon * 1000) : undefined;
    let updatedAt = editedon ? new Date(editedon * 1000) : undefined;


    const query = target.getQuery("Resource")

    await query.update({
      createdAt,
      updatedAt,
    })
      .where({
        id: objectId,
      })
      .then();

    // console.log(chalk.green("update query SQL"), query.toString());

    return result;
  }

  /**
   * Eof Import Blogs
   */


  /**
   * Import Topics
   */
  async importTopics() {

    this.log("Импортируем топики", "Info");

    // throw new Error("Test");

    const {
      source,
      target,
      ctx,
    } = this;



    const query = source.getQuery("site_content", "source")
      ;

    query
      .leftJoin(target.getTableName("Resource", "target"), "target.oldID", "source.id")
      .innerJoin(target.getTableName("User"), "User.oldID", "source.createdby")
      .whereNull("target.id")
      .whereIn("source.template", [
        15,
      ])
      ;

    query
      .innerJoin(source.getTableName("society_blog_topic", "blog_topic"), "blog_topic.topicid", "source.id")

    query
      .innerJoin(target.getTableName("Resource", "Blog"), "Blog.oldID", "blog_topic.blogid")


    query.select([
      "source.*",
      "Blog.id as blogId",
    ]);

    // query.limit(2);


    // console.log(chalk.green("query SQL"), query.toString());

    // throw new Error ("Topic error test");

    const objects = await query.then();

    // console.log("objects", objects);

    await this.log(`Было получено ${objects && objects.length} топиков`, "Info");

    // return;

    const processor = this.getProcessor(objects, this.writeTopic.bind(this));

    for await (const result of processor) {

      // console.log("writeUser result", result);

    }

  }


  async writeTopic(object) {

    // throw new Error("writeTopic error test");

    const {
      ctx,
      target,
    } = this

    const {
      db,
    } = ctx;

    let result;

    let {
      id,
      pagetitle: name,
      createdon,
      editedon,
      createdby,
      uri,
      published,
      deleted,
      hidemenu,
      searchable,
      blogId,
      content: text,
      class_key,
      template,
    } = object;

    let type = "Topic";

    let {
      content,
      contentText,
    } = this.getContent(text) || {};

    uri = this.prepareUri(uri);

    /**
     * Сохраняем объект
     */
    result = await db.mutation.createResource({
      data: {
        type,
        oldID: id,
        class_key,
        template,
        uri,
        name,
        content,
        contentText,
        published: published === 1,
        deleted: deleted === 1,
        hidemenu: hidemenu === 1,
        searchable: searchable === 1,
        CreatedBy: {
          connect: {
            oldID: createdby,
          },
        },
        Blog: blogId ? {
          connect: {
            id: blogId,
          },
        } : undefined,
      },
    });

    const {
      id: objectId,
    } = result;

    /**
     * Если пользователь был сохранен, надо обновить дату его создания
     */
    let createdAt = createdon ? new Date(createdon * 1000) : undefined;
    let updatedAt = editedon ? new Date(editedon * 1000) : undefined;


    const query = target.getQuery("Resource")

    await query.update({
      createdAt,
      updatedAt,
    })
      .where({
        id: objectId,
      })
      .then();

    // console.log(chalk.green("update query SQL"), query.toString());

    return result;
  }

  /**
   * Eof Import Topics
   */


  /**
   * Import Comments
   */
  async importComments() {

    this.log("Импортируем комментарии", "Info");


    const {
      source,
      target,
      ctx,
    } = this;

    const knex = source.getKnex();


    const query = source.getQuery("society_comments", "source")
      ;

    query
      .leftJoin(target.getTableName("Resource", "target"), "target.oldID", "source.id")
      .innerJoin(target.getTableName("User"), "User.oldID", "source.createdby")
      .whereNull("target.id")
      // .whereIn("template", [
      //   15,
      // ])
      ;

    query
      .innerJoin(source.getTableName("society_threads", "threads"), "threads.id", "source.thread_id")

    query
      .innerJoin(target.getTableName("Resource", "Topic"), "Topic.oldID", "threads.target_id")

    query
      .leftJoin(target.getTableName("Resource", "Parent"), "Parent.oldID", "source.parent")


    query.select([
      "source.*",
      "source.id as oldID",
      "target_id as topicId",
      "Topic.name as topicName",
      "Parent.id as parentId",
      knex.raw("unix_timestamp(source.createdon) as createdon"),
      knex.raw("unix_timestamp(source.editedon) as editedon"),
    ]);

    query.orderBy("source.id");

    // query.limit(2);

    // throw new Error("Comments error test");

    // console.log(chalk.green("query SQL"), query.toString());


    const objects = await query.then();

    // console.log("objects", objects);

    await this.log(`Было получено ${objects && objects.length} комментариев`, "Info");

    // return;

    const processor = this.getProcessor(objects, this.writeComment.bind(this));

    for await (const result of processor) {

      // console.log("writeUser result", result);

    }

  }


  async writeComment(object) {

    const {
      ctx,
      target,
    } = this

    const {
      db,
    } = ctx;

    let result;

    let {
      id,
      oldID,
      // pagetitle: name,
      createdon,
      editedon,
      createdby,
      // uri,
      // template,
      published,
      deleted,
      // hidemenu,
      // searchable,
      topicId,
      parentId,
      text,
      topicName,
      class_key,
      template,
    } = object;

    let type = "Comment";

    const uri = `/comments/comment-${oldID}.html`;

    let {
      content,
      contentText,
    } = this.getContent(text) || {};

    // console.log(chalk.green("content"), content);
    // console.log(chalk.green("contentText"), contentText);

    let name = contentText && contentText.substr(0, 50) || `Комментарий к топику ${topicName}`;

    // console.log(chalk.green("name"), name);

    // return;
    /**
     * Сохраняем объект
     */
    result = await db.mutation.createResource({
      data: {
        type,
        oldID: id,
        class_key,
        template,
        uri,
        name,
        content,
        contentText,
        published: published === 1,
        deleted: deleted === 1,
        // hidemenu: hidemenu === 1,
        // searchable: searchable === 1,
        CreatedBy: {
          connect: {
            oldID: createdby,
          },
        },
        CommentTarget: {
          connect: {
            oldID: topicId,
          },
        },
        Parent: parentId ? {
          connect: {
            id: parentId,
          },
        } : undefined,
      },
    });

    const {
      id: objectId,
    } = result;

    /**
     * Если пользователь был сохранен, надо обновить дату его создания
     */
    let createdAt = createdon ? new Date(createdon * 1000) : undefined;
    let updatedAt = editedon ? new Date(editedon * 1000) : undefined;


    const query = target.getQuery("Resource")

    await query.update({
      createdAt,
      updatedAt,
    })
      .where({
        id: objectId,
      })
      .then();

    // console.log(chalk.green("update query SQL"), query.toString());

    return result;
  }


  getContent(text) {

    // console.log(chalk.green("text"), text);

    let preparedContent;

    let content;

    let blocks;
    // let editorState;

    // throw new Error ("sdfsdf");

    // return;

    text = text && text.trim();

    if (text) {

      /**
       * Сначала пытаемся сконвертировать в объект, так как это может быть готовый контент
       */
      try {
        content = JSON.parse(text);

        // Если данные нормально парсятся, значит это контент 

      }
      catch (error) {

      }


      /**
       * Проверяем полученный контент.
       * Если не был получен контент или контент - число,
       * то создаем новый стейт
       */
      if (!content || typeof content === "number") {

        // Иначе конвертируем с исходного текста
        try {
          blocks = convertFromHTML(text, this.serverDOMBuilder);
        }
        catch (error) {

          console.error(chalk.red("convertFromHTML error"), error);
          console.error(chalk.red("convertFromHTML text"), text);

          throw error;
        }
        // console.log(chalk.green("blocks"), blocks);

        // Если блоки были получены, то создаем стейт контента

        if (blocks) {

          /**
           * Из полученных блоков создаем контент-стейт
           */

          let editorState;

          try {
            editorState = ContentState.createFromBlockArray(blocks);
          }
          catch (error) {
            console.error(chalk.red("editorState error"), error);
            console.error(chalk.red("editorState text"), text);
            console.error(chalk.red("editorState blocks"), blocks);
          }

          try {
            // конвертируем стейт в сырые данные для записи
            content = convertToRaw(editorState);
          }
          catch (error) {
            console.error(chalk.red("convertToRaw error"), error);
            console.error(chalk.red("convertToRaw text"), text);
            console.error(chalk.red("convertToRaw editorState"), editorState);
          }

          /**
           * Если контент не был получен, возвращаем пусто
           */
          if (!content) {

            return;
          }

        }
        else {
          console.error(chalk.red("Не были получены блоки контента"), text);
          throw new Error("Не были получены блоки контента");
        }

      }


      if (content) {

        const resourceProcessor = new ResourceProcessor(this.ctx);

        preparedContent = resourceProcessor.prepareContent({
          data: {
            content,
          },
        }, {});

        // console.log(chalk.green("preparedContent"), preparedContent);

        if (!preparedContent) {
          throw new Error("Не был получен контент");
        }

      }
      else {
        throw new Error("Не был получен контент");
      }

      /**
       * Если контент был получен, выдергиваем сырой текст
       */

    }


    return preparedContent;
  }

  serverDOMBuilder(html) {

    const doc = document.implementation.createHTMLDocument('div');

    doc.documentElement.innerHTML = html;

    const root = doc.getElementsByTagName('body')[0];

    return root;
  }

  prepareUri(uri) {

    if (!uri) {
      throw new Error("uri is empty");
    }

    if (!uri.startsWith("/")) {
      uri = `/${uri}`;
    }

    return uri;
  }

  /**
   * Eof Import Comments
   */


  /**
   * Import Tags
   */
  async importTags() {

    this.log("Импортируем теги", "Info");

    // throw new Error("Test");

    const {
      source,
      target,
      ctx,
    } = this;


    const knex = source.getKnex();

    const sessionQuery = knex.raw(`SET SESSION group_concat_max_len = 10000000;`);

    await sessionQuery.then();

    // console.log(chalk.green("sessionQuery SQL"), sessionQuery.toString());
    // return;

    // const query = source.getQuery("society_topic_tags", "source")
    //   ;

    // // let query = knex(source.getTableName("society_topic_tags", "tags"))
    // query
    //   .groupBy("tag");

    // query
    //   // .count("* as count")
    //   .select(knex.raw("GROUP_CONCAT(topic_id) as topic_ids"))
    //   .select("source.tag as name")
    //   // .where("source.active", 1)
    //   ;



    let tagsQuery = knex(source.getTableName("society_topic_tags", "tags"))
      .innerJoin(source.getTableName("site_content", "topic"), "topic.id", "tags.topic_id")
      // .count("* as count")
      .select(knex.raw("GROUP_CONCAT(topic_id) as topic_ids"))
      .select("tags.tag as name")
      // .where("tags.active", 1)
      .groupBy("tag")
      .as("source")
      ;

    let query = knex.from(tagsQuery)
    // .whereRaw(`tag.topic_id = topics.id`);

    query
      .leftJoin(target.getTableName("Tag", "target"), "target.name", "source.name")
      ;

    query.whereNull("target.id");

    query.select([
      "source.name",
      "source.topic_ids",
    ]);

    // query.limit(1);


    // console.log(chalk.green("query SQL"), query.toString());

    // throw new Error ("Topic error test");

    const objects = await query.then();

    // console.log("objects", objects);

    await this.log(`Было получено ${objects && objects.length} тегов`, "Info");

    // return;

    const processor = this.getProcessor(objects, this.writeTag.bind(this));

    for await (const result of processor) {

      // console.log("writeUser result", result);

    }

  }


  async writeTag(object) {

    // console.log(chalk.green("Create tag object"), object);
    // throw new Error("writeTopic error test");

    const {
      ctx,
      target,
    } = this

    const {
      db,
    } = ctx;

    let result;

    const {
      topic_ids,
      name,
    } = object;

    let resourcesIds = topic_ids && topic_ids.split(",").map(n => parseInt(n)) || [];


    /**
     * Сохраняем объект
     */
    result = await db.mutation.createTag({
      data: {
        name,
        CreatedBy: {
          connect: {
            username: "Fi1osof",
          },
        },
        Resources: resourcesIds && resourcesIds.length ? {
          create: resourcesIds.map(oldID => {
            return {
              Resource: {
                connect: {
                  oldID,
                },
              },
              CreatedBy: {
                connect: {
                  username: "Fi1osof",
                },
              },
            }
          }),
        } : undefined,
      },
    });


    return result;
  }

  /**
   * Eof Import Tags
   */


  /**
   * Import NotificationTypes
   */
  async importNotificationTypes() {

    this.log("Импортируем типы уведомлений", "Info");

    // throw new Error("Test");

    const {
      source,
      target,
      ctx,
    } = this;


    const knex = source.getKnex();


    const query = source.getQuery("society_notice_types", "source")
      ;

    query
      .leftJoin(target.getTableName("NotificationType", "target"), "target.oldID", "source.id")
      // .innerJoin(target.getTableName("User"), "User.oldID", "source.createdby")
      .whereNull("target.id")
      // .whereIn("template", [
      //   15,
      // ])
      ;

    query
      .select([
        "source.*",
      ]);


    // query.limit(1);


    // console.log(chalk.green("query SQL"), query.toString());

    // throw new Error ("Topic error test");

    const objects = await query.then();

    // console.log("objects", objects);

    await this.log(`Было получено ${objects && objects.length} типов уведомлений`, "Info");

    // return;

    const processor = this.getProcessor(objects, this.writeNotificationType.bind(this));

    for await (const result of processor) {

      // console.log("writeUser result", result);

    }

  }


  async writeNotificationType(object) {

    // console.log(chalk.green("writeNotificationType object"), object);
    // throw new Error("writeTopic error test");

    const {
      ctx,
      source,
      target,
    } = this

    const {
      db,
    } = ctx;

    let result;

    const {
      id: oldID,
      type: name,
      comment,
    } = object;


    // Получаем пользователей с этим уведомлением


    const query = source.getQuery("society_notice_users", "source")
      ;

    query
      .innerJoin(target.getTableName("User"), "User.oldID", "source.user_id")
      .where("notice_id", oldID)
      ;

    query.select([
      "User.id as userId",
    ]);

    // query.limit(3);

    // console.log(chalk.green("query SQL"), query.toString());


    const users = await query.then();

    // console.log("objects", objects);

    await this.log(`Было получено ${users && users.length} пользователей-уведомлений`, "Info");


    /**
     * Сохраняем объект
     */
    result = await db.mutation.createNotificationType({
      data: {
        oldID,
        name,
        comment,
        CreatedBy: {
          connect: {
            username: "Fi1osof",
          },
        },
        Users: users && users.length ? {
          connect: users.map(({ userId }) => {
            return {
              id: userId,
            }
          }),
        } : undefined,
      },
    });


    return result;
  }

  /**
   * Eof Import NotificationTypes
   */


  /**
   * Import Teams
   */
  async importTeams() {

    this.log("Импортируем команды", "Info");

    // throw new Error("Test");

    const {
      source,
      target,
      ctx,
    } = this;


    const knex = source.getKnex();


    const query = source.getQuery("modxsite_companies", "source")
      ;

    query
      .leftJoin(target.getTableName("Team", "target"), {
        "target.oldID": "source.id",
      })
      .innerJoin(target.getTableName("User"), "User.oldID", "source.createdby")
      .leftJoin(target.getTableName("User", "Owner"), "Owner.oldID", "source.owner")
      .innerJoin(source.getTableName("site_content", "resource"), function () {

        this
          .on({
            "resource.id": "source.resource_id",
            "resource.parent": 1015,
          })

      })
      .whereNull("target.id")

      // .whereNotNull("Owner.id")
      // .whereNot("Owner.id", "cjodr6nytah090850o0ieoieg")
      ;


    query.select([
      "source.*",
      "source.id as oldID",
      "User.id as createdById",
      "resource.uri",
      "resource.deleted",
      "resource.published",
      "resource.hidemenu",
      "resource.searchable",
      "resource.createdon",
      "resource.editedon",
      "Owner.id as ownerId",
    ]);

    // query.limit(1);


    // console.log(chalk.green("query SQL"), query.toString());

    // throw new Error ("Topic error test");

    const objects = await query.then();

    // console.log("objects", objects);

    await this.log(`Было получено ${objects && objects.length} команд`, "Info");

    // return;

    const processor = this.getProcessor(objects, this.writeTeam.bind(this));

    for await (const result of processor) {

      // console.log("writeUser result", result);

    }

  }


  async writeTeam(object) {

    const {
      ctx,
      target,
    } = this

    const {
      db,
    } = ctx;

    let result;

    let {
      id,
      oldID,
      name,
      longtitle,
      createdon,
      editedon,
      createdById,
      ownerId,
      uri,
      published,
      deleted,
      hidemenu,
      searchable,
      content: text,
      class_key,
      template,
      status,
      address,
      website,
      email,
      phone,
    } = object;

    let type = "Team";

    createdById = ownerId || createdById;

    let {
      content,
      contentText,
    } = this.getContent(text) || {};

    uri = this.prepareUri(uri);


    website = website && website.trim() || null;

    if (website && !website.match(/^http.*?:\/\//)) {
      website = `http://${website}`;
    }

    /**
     * Сохраняем объект
     */
    result = await db.mutation.createTeam({
      data: {
        oldID,
        name,
        status: status === 1 ? "Active" : "Inactive",
        address: address && address.trim() || null,
        website,
        email: email && email.trim() || null,
        phone: phone && phone.trim() || null,
        CreatedBy: {
          connect: {
            id: createdById,
          },
        },
        Resource: {
          create: {
            type,
            uri,
            name,
            longtitle,
            class_key,
            template,
            content,
            contentText,
            published: published === 1,
            deleted: deleted === 1,
            hidemenu: hidemenu === 1,
            searchable: searchable === 1,
            CreatedBy: {
              connect: {
                id: createdById,
              },
            },
          },
        },
      },
    });

    const {
      id: objectId,
    } = result;

    /**
     * Если пользователь был сохранен, надо обновить дату его создания
     */
    let createdAt = createdon ? new Date(createdon * 1000) : undefined;
    let updatedAt = editedon ? new Date(editedon * 1000) : undefined;


    const query = target.getQuery("Team")

    await query.update({
      createdAt,
      updatedAt,
    })
      .where({
        id: objectId,
      })
      .then();

    // console.log(chalk.green("update query SQL"), query.toString());

    return result;
  }

  /**
   * Eof Import Teams
   */


  /**
   * Import Services
   */
  async importServices() {

    this.log("Импортируем услуги", "Info");

    // throw new Error("Test");

    const {
      source,
      target,
      ctx,
    } = this;


    const knex = source.getKnex();


    const query = source.getQuery("site_content", "source")
      ;

    query
      // .leftJoin(target.getTableName("Resource", "target"), function () {

      //   this
      //     .on({
      //       "target.oldID": "source.id",
      //     })
      //     .on(knex.raw(`target.type = 'Service'`))

      // })
      .leftJoin(target.getTableName("Service", "target"), {
        "target.oldID": "source.id",
      })
      .innerJoin(target.getTableName("User"), "User.oldID", "source.createdby")
      .where("source.parent", 1473)
      .whereNull("target.id")
      ;


    query.select([
      "source.*",
      "User.id as createdById",
    ]);

    // query.limit(1);


    // console.log(chalk.green("query SQL"), query.toString());

    // throw new Error ("Topic error test");

    const objects = await query.then();

    // console.log("objects", objects);

    await this.log(`Было получено ${objects && objects.length} услуг`, "Info");

    // return;

    const processor = this.getProcessor(objects, this.writeService.bind(this));

    for await (const result of processor) {

      // console.log("writeUser result", result);

    }

  }


  async writeService(object) {

    const {
      ctx,
      target,
    } = this

    const {
      db,
    } = ctx;

    let result;

    let {
      id,
      pagetitle: name,
      longtitle,
      createdon,
      editedon,
      createdby,
      uri,
      published,
      deleted,
      hidemenu,
      searchable,
      content: text,
      class_key,
      template,
    } = object;

    let type = "Service";

    let {
      content,
      contentText,
    } = this.getContent(text) || {};

    uri = this.prepareUri(uri);

    /**
     * Сохраняем объект
     */
    result = await db.mutation.createService({
      data: {
        oldID: id,
        name,
        CreatedBy: {
          connect: {
            oldID: createdby,
          },
        },
        Resource: {
          create: {
            type,
            uri,
            name,
            longtitle,
            class_key,
            template,
            content,
            contentText,
            published: published === 1,
            deleted: deleted === 1,
            hidemenu: hidemenu === 1,
            searchable: searchable === 1,
            CreatedBy: {
              connect: {
                oldID: createdby,
              },
            },
          },
        },
      },
    });

    const {
      id: objectId,
    } = result;

    /**
     * Если пользователь был сохранен, надо обновить дату его создания
     */
    let createdAt = createdon ? new Date(createdon * 1000) : undefined;
    let updatedAt = editedon ? new Date(editedon * 1000) : undefined;


    const query = target.getQuery("Service")

    await query.update({
      createdAt,
      updatedAt,
    })
      .where({
        id: objectId,
      })
      .then();

    // console.log(chalk.green("update query SQL"), query.toString());

    return result;
  }

  /**
   * Eof Import Services
   */


  /**
   * Import Projects
   */
  async importProjects() {

    this.log("Импортируем проекты", "Info");

    // throw new Error("Test");

    const {
      source,
      target,
      ctx,
    } = this;


    const knex = source.getKnex();


    const query = source.getQuery("site_content", "source")
      ;

    query
      .leftJoin(target.getTableName("Project", "target"), {
        "target.oldID": "source.id",
        // "target.template": 29,
      })
      .innerJoin(target.getTableName("User"), "User.oldID", "source.createdby")
      .leftJoin(source.getTableName("site_tmplvar_contentvalues", "tv_image"), function () {
        this
          .on("tv_image.tmplvarid", 3)
          .on("tv_image.contentid", "source.id")
          .on(knex.raw(`tv_image.value > ''`))
          ;
      })
      .leftJoin(source.getTableName("site_tmplvar_contentvalues", "tv_developer"), function () {
        this
          .on("tv_developer.tmplvarid", 11)
          .on("tv_developer.contentid", "source.id")
          .on(knex.raw(`tv_developer.value > ''`))
          ;
      })
      .leftJoin(source.getTableName("modxsite_companies", "companies"), {
        "companies.resource_id": "tv_developer.value",
      })
      .leftJoin(target.getTableName("Team", "team"), {
        "team.oldID": "companies.id",
        // "target.template": 29,
      })
      .where("source.parent", 1443)
      .whereNull("target.id")
      // .whereNotNull("tv_developer.id")
      // .whereNotNull("team.id")
      ;

    query.select([
      "source.*",
      "User.id as createdById",
      "tv_image.value as image",
      "team.id as teamId",
    ]);

    // query.limit(1);


    // console.log(chalk.green("query SQL"), query.toString());

    // throw new Error ("Topic error test");

    const objects = await query.then();

    // console.log("objects", objects);

    await this.log(`Было получено ${objects && objects.length} проектов`, "Info");

    // return;

    const processor = this.getProcessor(objects, this.writeProject.bind(this));

    for await (const result of processor) {

      // console.log("writeUser result", result);

    }

  }


  async writeProject(object) {

    const {
      ctx,
      target,
    } = this

    const {
      db,
    } = ctx;

    let result;

    let {
      id: oldID,
      pagetitle: name,
      longtitle,
      createdon,
      editedon,
      createdby,
      uri,
      published,
      deleted,
      hidemenu,
      searchable,
      content: text,
      class_key,
      template,
      image,
      teamId,
    } = object;

    let type = "Project";

    let {
      content,
      contentText,
    } = this.getContent(text) || {};

    uri = this.prepareUri(uri);

    name = longtitle && longtitle.trim() || name;

    name = name.trim().replace(/^\/|\/$/g, '');

    let url = name;

    if (!url.match(/^http.*?:\/\//)) {
      url = `http://${url}`;
    }


    name = punycode.toUnicode(name);

    name = name.replace(/^http.*?:\/\//, '');

    /**
     * Получаем всех участников проекта
     */

    const {
      source,
    } = this;


    const knex = source.getKnex();

    const membersQuery = source.getQuery("modxsite_projects_members", "source")
      ;

    membersQuery
      .where("source.project_id", oldID)
      .innerJoin(target.getTableName("User"), "User.oldID", "source.user_id")
      .innerJoin(target.getTableName("Service"), "Service.oldID", "source.service_id")
      // .whereNull("target.id")
      ;


    membersQuery.select([
      // "source.*",
      "source.project_id",
      "source.user_id",
      knex.raw("GROUP_CONCAT(service_id) as services"),
      // "User.id as createdById",
    ]);

    membersQuery.groupBy(1);
    membersQuery.groupBy(2);

    // query.limit(1);


    // console.log(chalk.green("membersQuery SQL"), membersQuery.toString());

    // throw new Error ("Topic error test");

    const objects = await membersQuery.then();

    // console.log("objects", objects);

    await this.log(`Было получено ${objects && objects.length} участников проекта`, "Info");

    // return;

    let Members = {
      create: objects.map(n => {

        const {
          status,
          user_id,
          services,
        } = n;

        return {
          status: status === 1 || status === undefined ? "Active" : "Invited",
          User: {
            connect: {
              oldID: user_id,
            },
          },
          Services: {
            connect: services.split(",").map(service_id => {
              return {
                oldID: parseInt(service_id),
              };
            }),
          },
          CreatedBy: {
            connect: {
              oldID: createdby,
            },
          },
        }
      }),
    }


    let Image;

    if (image && fs.existsSync(`uploads/${image}`)) {

      // console.log("image", image);

      const filename = path.basename(image);
      // // console.log("image.basename", path.basename(image));
      // // console.log("image.format", path.format(image));
      // // console.log("image.extname", path.extname(image));

      // console.log("mime", mime.getType(image));

      const mimetype = mime.getType(image);

      Image = {
        create: {
          path: image,
          filename,
          mimetype,
          encoding: "7bit",
          CreatedBy: {
            connect: {
              oldID: createdby,
            },
          },
        },
      }

    }

    // return;

    /**
     * Сохраняем объект
     */
    result = await db.mutation.createProject({
      data: {
        oldID,
        name,
        url,
        CreatedBy: {
          connect: {
            oldID: createdby,
          },
        },
        Resource: {
          create: {
            type,
            uri,
            name,
            longtitle,
            class_key,
            template,
            content,
            contentText,
            published: published === 1,
            deleted: deleted === 1,
            hidemenu: hidemenu === 1,
            searchable: searchable === 1,
            Image,
            CreatedBy: {
              connect: {
                oldID: createdby,
              },
            },
          },
        },
        Members,
        Team: teamId ? {
          connect: {
            id: teamId,
          },
        } : undefined,
      },
    });

    const {
      id: objectId,
    } = result;

    /**
     * Если пользователь был сохранен, надо обновить дату его создания
     */
    let createdAt = createdon ? new Date(createdon * 1000) : undefined;
    let updatedAt = editedon ? new Date(editedon * 1000) : undefined;


    const query = target.getQuery("Project")

    await query.update({
      createdAt,
      updatedAt,
    })
      .where({
        id: objectId,
      })
      .then();

    // console.log(chalk.green("update query SQL"), query.toString());

    return result;
  }

  /**
   * Eof Import Projects
   */




}

