
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

    await this.importUsers();
    await this.importBlogs();
    await this.importTopics();
    await this.importComments();
    // await this.importTags();
    // await this.importVotes();

  }

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

    const targetUsersTable = target.getTableName("User", "targetUser");


    const query = source.getQuery("users", "users")
      .innerJoin(source.getTableName("user_attributes", "profile"), "profile.internalKey", "users.id")
      .leftJoin(source.getTableName("society_user_attributes"), "society_user_attributes.internalKey", "users.id")
      .leftJoin(targetUsersTable, "targetUser.oldID", "users.id")
      ;

    query.whereNull("targetUser.oldID");

    query.select([
      "users.*",
      "profile.fullname",
      "profile.email",
      "society_user_attributes.createdon as society_user_createdon",
      "users.createdon as user_createdon",
    ]);

    // query.limit(1);


    // console.log(chalk.green("query SQL"), query.toString());

    // return;

    const users = await query.then();

    // console.log("users", users);

    await this.log(`Было получено ${users && users.length} пользователей`, "Info");

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

    const {
      id,
      username,
      fullname,
      email,
      society_user_createdon,
      user_createdon,
    } = user;

    /**
     * Сохраняем пользователя
     */
    result = await db.mutation.createUser({
      data: {
        oldID: id,
        username,
        fullname,
        email,
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

    await query.update({
      createdAt,
    })
      .where({
        id: userId,
      })
      .then();

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

    const {
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

    const {
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

    const {
      id,
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

    const uri = `/comments/comment-${id}.html`;

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
          if(!content){
            
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

  /**
   * Eof Import Comments
   */




}

