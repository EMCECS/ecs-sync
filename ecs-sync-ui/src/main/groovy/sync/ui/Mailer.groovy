package sync.ui

trait Mailer {
    def mailService

    def simpleMail(add, sub, body) {
        try {
            mailService.sendMail {
                to add
                subject sub
                text body
            }
        } catch (Throwable t) {
            log.error("could not send mail [subject: ${sub}]", t)
        }
    }
}
