# Core Python module 
import streamlit_authenticator as stauth

# لیست رمزهای عبور
passwords = ['1234', '5678']

# هش کردن رمزها
hashed_passwords = stauth.Hasher(passwords).generate()

# چاپ هش‌ها
for hashed in hashed_passwords:
    print(hashed)



# usernames = ['user1', 'user2']
# passwords = ['12280967mnm', '9522990133hmnm']
# names = ['user1', 'user2']


# hashed_passwords = stauth.Hasher(passwords).generate()

# authenticator = stauth.Authenticate(names, usernames, hashed_passwords,
#     'some_cookie_name', 'some_signature_key', cookie_expiry_days=30)