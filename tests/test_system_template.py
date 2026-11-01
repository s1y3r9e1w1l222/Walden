"""Synthetic system templates with explicitly verified profile links."""
import unittest
from wechat_local.event_normalizer import _structured_content

TEMPLATE='''<sysmsg type="sysmsgtemplate"><sysmsgtemplate><content_template type="tmpl_type_profile"><plain/><template>"$adder$" scanned "$from$" and joined</template><link_list><link name="adder" type="link_profile"><memberlist><member><username>private-user-a</username><nickname>Alpha</nickname></member></memberlist></link><link name="from" type="link_profile"><memberlist><member><username>private-user-b</username><nickname>Beta</nickname></member></memberlist></link></link_list></content_template></sysmsgtemplate></sysmsg>'''

class SystemTemplateTests(unittest.TestCase):
    def test_profile_template_resolves_nicknames_only(self):
        kind,text,_=_structured_content(10000,TEMPLATE,set())
        self.assertEqual(kind,'system');self.assertEqual(text,'"Alpha" scanned "Beta" and joined')
        self.assertNotIn('private-user',text)
    def test_unknown_or_invalid_template_does_not_copy_raw_identifiers(self):
        for xml in (TEMPLATE.replace('name="from"','name="other"'),TEMPLATE.replace('type="link_profile"','type="link_url"',1),TEMPLATE.replace('<nickname>Alpha</nickname>','<nickname><x>Alpha</x></nickname>'),TEMPLATE.replace('</link_list>','<link name="adder" type="link_profile"/></link_list>'),TEMPLATE.replace('$from$','$missing$')):
            self.assertIsNone(_structured_content(10000,xml,set())[1])
    def test_nickname_may_equal_username_and_text_may_contain_a_url(self):
        xml=TEMPLATE.replace('Alpha','private-user-a').replace('scanned','saw https://example.invalid and scanned')
        text=_structured_content(10000,xml,set())[1]
        self.assertEqual(text,'"private-user-a" saw https://example.invalid and scanned "Beta" and joined')
    def test_dollar_in_nickname_is_not_reinterpreted_as_a_placeholder(self):
        self.assertEqual(_structured_content(10000,TEMPLATE.replace('Alpha','A$B'),set())[1],'"A$B" scanned "Beta" and joined')
    def test_ordinary_content_and_plain_system_preserved(self):
        self.assertEqual(_structured_content(10000,'<sysmsg><content>Notice</content></sysmsg>',set())[1],'Notice')
        self.assertEqual(_structured_content(10000,'Plain notice',set())[1],'Plain notice')
    def test_existing_content_precedes_template(self):
        xml=TEMPLATE.replace('<sysmsgtemplate>','<content>Ordinary</content><sysmsgtemplate>',1)
        self.assertEqual(_structured_content(10000,xml,set())[1],'Ordinary')
    def test_profile_plain_precedes_template(self):
        xml=TEMPLATE.replace('<plain/>','<plain>Resolved notice</plain>')
        self.assertEqual(_structured_content(10000,xml,set())[1],'Resolved notice')
    def test_unicode_template_and_names_are_preserved(self):
        xml=TEMPLATE.replace('Alpha','甲').replace('Beta','乙').replace('scanned','通过扫描').replace('and joined','分享的二维码加入群聊')
        self.assertEqual(_structured_content(10000,xml,set())[1],'"甲" 通过扫描 "乙" 分享的二维码加入群聊')
