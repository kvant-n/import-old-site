
import chalk from "chalk";
import PrismaProcessor from "@prisma-cms/prisma-processor";

import MySQL from "./mysql";

export default class ImportProcessor extends PrismaProcessor {


  constructor(props) {

    super(props);

    this.objectType = "Import";

  }


  async create(method, args, info) {

    // console.log("create args", args);

    // return super.create(method, args, info);

    const {
      currentUser,
      db,
    } = this.ctx;



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
        });

      if (result) {
        writed++;
      }
      else {
        skiped++;
      }

      yield result;
    }

    await this.log(`Записано: ${writed}, пропущено: ${skiped}, ошибок: ${errors}`, "Info");

  }


  async processImport(args) {

    await this.initDB(args);

    // await this.importUsers();
    // await this.importBlogs();
    await this.importTopics();
    // await this.importComments();

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

    query.limit(1);


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
      .whereIn("template", [
        14,
        16,
      ])
      ;


    query.select([
      "source.*",
    ]);

    query.limit(1);


    console.log(chalk.green("query SQL"), query.toString());

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
      template,
      published,
      deleted,
      hidemenu,
      searchable,
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

    /**
     * Сохраняем объект
     */
    result = await db.mutation.createResource({
      data: {
        type,
        oldID: id,
        uri,
        name,
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
      .whereIn("template", [
        15,
      ])
      ;

    query
      .leftJoin(source.getTableName("society_blog_topic", "blog_topic"), "blog_topic.topicid", "source.id")

    query
      .leftJoin(target.getTableName("Resource", "Blog"), "Blog.oldID", "blog_topic.blogid")


    query.select([
      "source.*",
      "Blog.id as blogId",
    ]);

    query.limit(1);


    // console.log(chalk.green("query SQL"), query.toString());


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
      template,
      published,
      deleted,
      hidemenu,
      searchable,
      blogId,
    } = object;

    let type = "Topic";

    /**
     * Сохраняем объект
     */
    result = await db.mutation.createResource({
      data: {
        type,
        oldID: id,
        uri,
        name,
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




}

