import { SESClient } from "@aws-sdk/client-ses";
import { S3Client, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { IncomingWebhook } from '@slack/webhook';
import mysql from 'mysql2/promise';

const slackWebhookUrl = 'https://hooks.slack.com/services/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'; // Slack Webhook URLを設定してください

const slackWebhook = new IncomingWebhook(slackWebhookUrl);

const s3Folder = "inquiry_folder";
const s3Q_IdFile = "Q_Id.txt";
const s3ErrQ_IdFile = "errorQ_Data.txt";
const s3StatusFile = "status.txt";

const S3_FINISH_CODE = "0";
const S3_RUNNING_CODE = "1";

const s3Client = new S3Client({
  region: 'ap-northeast-1',
  credentials: {
    accessKeyId: process.env.ACCESS_KEY_ID,
    secretAccessKey: process.env.SECRET_ACCESS_KEY
  }
});

let connection = null;

export const handler = async (event) => {
  // await slackWebhook.send({
  //     text: "test"
  //   });
  let processStatus = await getStatus();
  if (getErrorStatus(processStatus)) {
    await putStatusToS3(S3_FINISH_CODE);
    return;
  }

  await putStatusToS3(S3_RUNNING_CODE);

  const lastTimeData = await getDataFromS3(s3Q_IdFile);
  console.log(lastTimeData);
  if (await getErrorS3Data(lastTimeData)) {
    await putStatusToS3(S3_FINISH_CODE);
    return;
  }
  const s3Q_Id = lastTimeData[0];
  await createDbConnection();

  const rows = await getDataFromDb(s3Q_Id);
  if (await getErrorNewQuestionData(rows)) {
    await putStatusToS3(S3_FINISH_CODE);
    return;
  }

  const lastTimenum = lastTimeData[1];
  let numbering = Number(lastTimenum);
  for (var i = 0; i < rows.length; i++) {
    numbering = numbering + 1;
    var row = rows[i];
    await sendToSlack(row, numbering);
  }

  if (rows.length > 0) {
    const lastuser = rows.slice(-1)[0];
    const lastanswer_id = lastuser.answer_id;
    await putDataToS3(lastanswer_id, numbering, s3Q_IdFile);
  }
  await putStatusToS3(S3_FINISH_CODE);
};

const getStatus = async () => {
  const data = await s3Client.send(new GetObjectCommand({
    Bucket: process.env.BUCKET,
    Key: `${s3Folder}/${s3StatusFile}`
  }));
  const statusTextData = await data.Body.transformToString();
  return statusTextData.split('\n');
};

const getErrorStatus = (processStatus) => {
  if (!processStatus) {
    console.log("S3から処理対象の問い合わせIDの取得に失敗しました。");
    return true;
  } else if (processStatus == "1") {
    console.log("多重起動を検知しました。処理を終了します。");
    return true;
  }
  return false;
};

const putStatusToS3 = async (status) => {
  await s3Client.send(new PutObjectCommand({
    Bucket: process.env.BUCKET,
    Key: `${s3Folder}/${s3StatusFile}`,
    Body: status
  }));
};

const getDataFromS3 = async (filepath) => {
  const data = await s3Client.send(new GetObjectCommand({
    Bucket: process.env.BUCKET,
    Key: `${s3Folder}/${filepath}`
  }));
  const idTextData = await data.Body.transformToString();
  return idTextData.split('\n');
};

const createDbConnection = async () => {
  connection = await mysql.createConnection({
    host: process.env.MYSQL_HOST,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DBNAME
  });
};

const getDataFromDb = async (minQId) => {
  const [rows] = await connection.query({
    sql:
      `
  select 
  questionnaire_answers.id as 'answer_id',
  JSON_UNQUOTE(JSON_EXTRACT(questionnaire_answers.answers, '$[0].value')) as 'co_name',
  JSON_UNQUOTE(JSON_EXTRACT(questionnaire_answers.answers, '$[1].value')) as 'mail',
        CONCAT_WS(', ',
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(questionnaire_answers.answers, '$[2].value')) LIKE '%2abbb052-9fa0-4eac-b465-2e6244abee5a%' THEN '管理画面操作について' END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(questionnaire_answers.answers, '$[2].value')) LIKE '%b78b317c-88aa-4a4a-9495-c73be242c428%' THEN 'アカウントについて（ログインできないなど）' END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(questionnaire_answers.answers, '$[2].value')) LIKE '%1ec61ce2-8bc8-430b-b0ad-ae98da946bf9%' THEN 'アプリ公開後の情報の差し替えについて' END,
            CASE WHEN JSON_UNQUOTE(JSON_EXTRACT(questionnaire_answers.answers, '$[2].value')) LIKE '%1bbed063-8eb9-4486-9229-51fec07603bb%' THEN 'その他問い合わせ' END
        ) as 'q_type',
  JSON_UNQUOTE(JSON_EXTRACT(questionnaire_answers.answers, '$[3].value')) as 'detail'
  from questionnaire_answers
  where questionnaire_answers.content_id = 217710
      `,
    values: minQId,
    timeout: 500000
  });
  return rows;
};

const getErrorS3Data = async (data) => {
 console.log(data.length)
  if (data.length !== 2) {
    await sendErrorToSlack(`s3:${s3Folder}フォルダの${s3Q_IdFile}に不要データが登録されている可能性があります。処理を終了しました。`);
    console.log(`s3:${s3Folder}フォルダの${s3Q_IdFile}に不要データが登録されている可能性があります。処理を終了しました`);
    return true;
  }
  return false;
};

const getErrorNewQuestionData = async (data) => {
  if (!data) {
    await sendErrorToSlack("DBから問い合わせデータの取得に失敗しました。");
    console.log("DBから問い合わせデータの取得に失敗しました。");
    return true;
  } else if (data.length === 0) {
    console.log("新しい問い合わせデータはありませんでした");
    return true;
  }
  return false;
};

const putDataToS3 = async (lastId, num, filepath) => {
  await s3Client.send(new PutObjectCommand({
    Bucket: process.env.BUCKET,
    Key: `${s3Folder}/${filepath}`,
    Body: lastId + '\n' + num
  }));
};

const putErrorDataToS3 = async (Q_Id, filepath) => {
  await s3Client.send(new PutObjectCommand({
    Bucket: process.env.BUCKET,
    Key: `${s3Folder}/${filepath}`,
    Body: Q_Id
  }));
};

const sendToSlack = async (row, numbering) => {
  try {
    const slackMessage = `
      <!channel> *【問い合わせ通知】出展者より問い合わせがありました*
      *【企業名】* ${row.co_name}
      *【担当者メールアドレス】:* ${row.mail}
      *【問い合わせ内容】:* ${row.q_type}
      *【問い合わせ詳細】:* ${row.datail}
    `;
    await slackWebhook.send({
      text: slackMessage
    });
  } catch (error) {
    console.error('Slackへのメッセージ送信エラー:', error);
    const s3ErrQ_List = await getDataFromS3(s3ErrQ_IdFile);
    s3ErrQ_List.push(row.answer_id);
    await putErrorDataToS3(s3ErrQ_List, s3ErrQ_IdFile);
  }
};

const sendErrorToSlack = async (error_detail) => {
  const slackMessage = `*R&D問い合わせ通知エラー*\n${error_detail}`;
  await slackWebhook.send({
    text: slackMessage
  });
};
